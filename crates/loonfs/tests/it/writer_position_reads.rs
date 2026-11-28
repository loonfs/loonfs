//! Reads in a runtime with the namespace's writer session, which start from
//! the session's position instead of the read cache.

use crate::common::is_manifest_head;
use loonfs::metrics::DefaultMetricsRecorder;
use loonfs::test_support::{counter, writer};
use loonfs::{
    Commit, Consistency, ErrorCode, FoldWalOutcome, LoonFs, MetadataCache, Namespace, ReadOnly,
    SharedObjectStore, Writable,
};
use loonfs_core::limits::MAX_POSITION_MOVES;
use loonfs_objectstore::keys::{boundary, metadata_manifest_object, metadata_manifest_prefix};
use loonfs_test_support::clock::{bounded, ManualClock};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::format::manifest::MetadataRowFamily;
use loonfs_types::{ChecksumAlgorithm, ManifestNo, NamespaceId};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// The HEAD request that ends a load of the namespace: the check of the
    /// loaded manifest's successor, which follows the load's WAL reads. The
    /// probe of the load's discovery comes before those reads, so it isn't
    /// parked.
    LoadChecks,
    /// GET requests for the namespace's WAL objects.
    WalReads,
}

/// Creates a writer runtime with a one-second maximum staleness, a namespace
/// with a 1 KiB file, and a reader on the writer's runtime.
pub(crate) async fn fixture(name: &str, parked: Parked) -> Fixture {
    fixture_with_cache(name, parked, MetadataCache::default()).await
}

/// Creates a fixture like [`fixture`] whose runtime reads through
/// `metadata_cache`.
async fn fixture_with_cache(name: &str, parked: Parked, metadata_cache: MetadataCache) -> Fixture {
    let namespace_id = namespace_id(name);
    let memory = MemoryStore::new(ChecksumAlgorithm::Crc64nvme);
    let wal = loonfs_objectstore::keys::wal_prefix(&namespace_id);
    let blocking = match parked {
        Parked::WalPuts => BlockingStore::new(
            memory,
            KeyPredicate::prefix(wal),
            OperationSelector::PutCreateIfAbsent,
        ),
        Parked::ManifestChecks => BlockingStore::new(
            memory,
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::Head,
        ),
        Parked::LoadChecks => {
            let manifests = metadata_manifest_prefix(&namespace_id);
            // Whether a WAL read came after the last other read.
            let wal_read = AtomicBool::new(false);
            BlockingStore::matching(memory, move |operation| {
                if OperationSelector::Read.matches(operation.kind()) {
                    wal_read.store(operation.key().starts_with(&wal), Ordering::SeqCst);
                }
                operation.key().starts_with(&manifests)
                    && OperationSelector::Head.matches(operation.kind())
                    && wal_read.load(Ordering::SeqCst)
            })
        }
        Parked::WalReads => {
            BlockingStore::new(memory, KeyPredicate::prefix(wal), OperationSelector::Get)
        }
    };
    let recording = Arc::new(RecordingStore::new(blocking, KeyPredicate::any()));
    let store: SharedObjectStore = recording.clone();
    let clock = Arc::new(ManualClock::new(1_000_000));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = LoonFs::builder_with_store(store, loonfs::Context::Service)
        .writer_id(writer_id(name))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            max_staleness_ms: MAX_STALENESS_MS,
            ..loonfs::RuntimeConfig::default()
        })
        .monotonic_timer(clock.clone())
        .metrics_recorder(recorder.clone())
        .metadata_cache(metadata_cache)
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
    writer(fixture.recording.clone(), name).await
}

impl Fixture {
    pub(crate) async fn put(&self, path: &str) -> loonfs::Result<Commit> {
        self.namespace
            .put_file(path, b"file", &loonfs_test_support::test_actor())
            .await
    }

    /// Returns how many reads started from the writer's position and how
    /// many refreshed or loaded the namespace's head.
    fn head_sources(&self) -> (u64, u64) {
        let source = |label| {
            counter(
                &self.recorder.snapshot(),
                "loonfs.read_head.source",
                &[("source", label)],
            )
        };
        (source("position"), source("refreshed") + source("loaded"))
    }

    /// Returns how many lookups of a namespace head in the read cache hit
    /// and how many missed.
    fn head_cache_lookups(&self) -> (usize, usize) {
        let stats = self.writer.metadata_cache().stats();
        (stats.heads.hits, stats.heads.misses)
    }

    /// Returns whether `operation` is a request of a check of a manifest: a
    /// HEAD of a manifest, or a read of the manifest boundary.
    fn is_check(&self, operation: &RecordedOperation) -> bool {
        is_manifest_head(operation, &self.namespace_id)
            || operation.key() == boundary(&self.namespace_id)
    }

    /// Drops the session's position and the cached head the way a change of
    /// unknown effect does, so the next read loads the namespace from the
    /// store.
    pub(crate) async fn drop_position(&self) {
        loonfs::test_support::drop_namespace_view(&self.writer, &self.namespace_id).await;
    }

    /// Closes the writer session, so its position becomes the namespace's
    /// cached head.
    async fn close_session(&self) {
        self.writer
            .open_namespace(&self.namespace_id)
            .expect("a handle on the session")
            .close()
            .await
            .expect("close the session");
    }

    fn tail_replays(&self) -> u64 {
        counter(
            &self.recorder.snapshot(),
            "loonfs.publisher.tail_replays",
            &[],
        )
    }

    /// Asserts that the requests since the last reset start with a successor
    /// check of a manifest, that the walk after it loads that manifest and
    /// no later one, and that the read then reloads the namespace from its
    /// manifest boundary.
    fn assert_check_stopped_at_the_claim(&self) {
        let operations = self.recording.take();
        let Some(reload) = operations
            .iter()
            .position(|operation| operation.key() == boundary(&self.namespace_id))
        else {
            panic!("no reload from the manifest boundary: {operations:?}");
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
/// only the session's check of the position's manifest: a HEAD and a read of
/// the manifest boundary.
#[tokio::test]
async fn strong_reads_after_a_commit_send_one_manifest_check_each() {
    let fixture = fixture("position-reads", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    let entry = fixture
        .reader
        .strong()
        .stat("/next.txt")
        .await
        .expect("stat");
    assert_eq!(entry.head_seq, commit.committed_seq);
    let file = fixture
        .reader
        .strong()
        .read_file("/small.txt")
        .await
        .expect("read");
    assert_eq!(file.bytes, [b's'; 1024]);
    let operations = fixture.recording.take();
    assert!(
        operations.len() == 4
            && operations
                .iter()
                .all(|operation| fixture.is_check(operation)),
        "{operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions + 2, anchors));
}

/// Checks that an `eventual` read in the writer's runtime within the
/// runtime's maximum staleness of the position's last
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
/// position's manifest. Building the read context and preparing content
/// before a write read the position as it is, because the position includes
/// every commit of the session, and another writer's takeover fences the
/// session at the write's own publish.
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
        let entry = bounded(NO_WAIT, fixture.reader.strong().stat("/before.txt"))
            .await
            .expect("the read doesn't wait for the publish")
            .expect("stat");
        assert_eq!(entry.head_seq, before.committed_seq);
        let missing = bounded(NO_WAIT, fixture.reader.strong().stat("/during.txt"))
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
                .all(|operation| fixture.is_check(operation)),
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
    for (name, advance_ms, consistency) in [
        ("takeover-strong", 0, Consistency::Strong),
        ("takeover-eventual", MAX_STALENESS_MS, Consistency::Eventual),
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
            .open_namespace(&fixture.namespace_id)
            .expect("open namespace")
            .fold_wal()
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
        let reader = match consistency {
            Consistency::Eventual => fixture.reader.clone(),
            Consistency::Strong => fixture.reader.strong(),
        };
        let entry = reader
            .stat("/peer.txt")
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
/// read reloads the namespace from its manifest boundary.
///
/// The test support's same-epoch publisher folds each of the session's
/// commits, one fold more than the cap, under the session's writer epoch, so
/// the walk loads one manifest past the cap and stops there. In a runtime,
/// at most one publication of each kind is put and not yet recorded, so the
/// session's own publications never make a walk this long. Only a store that
/// breaks the format, or a publisher at the session's epoch that the design
/// doesn't allow, reaches the cap, and the cap is there so that such a store
/// can't make the walk run without end.
#[tokio::test]
async fn a_walk_past_its_cap_drops_the_position() {
    let fixture = fixture("walk-cap", Parked::WalPuts).await;
    let store: SharedObjectStore = fixture.recording.clone();
    for index in 0..=MAX_POSITION_MOVES {
        fixture
            .put(&format!("/fill-{index}.txt"))
            .await
            .expect("commit");
        let folded =
            loonfs_core::test_support::namespace_store(store.clone(), &fixture.namespace_id)
                .fixture_writer_session()
                .await
                .expect("the writer's epoch")
                .fold()
                .await
                .expect("fold");
        assert_eq!(
            folded.response.outcome,
            loonfs_core::FoldWalOutcome::Published
        );
    }
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    fixture
        .reader
        .strong()
        .stat("/small.txt")
        .await
        .expect("stat");
    let operations = fixture.recording.take();
    let reload = operations
        .iter()
        .position(|operation| operation.key() == boundary(&fixture.namespace_id))
        .expect("a reload from the manifest boundary");
    let walked = operations[..reload]
        .iter()
        .filter(|operation| matches!(operation, RecordedOperation::Get { .. }))
        .count();
    assert_eq!(walked, MAX_POSITION_MOVES + 1, "{operations:?}");
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Checks that a fold that this runtime publishes while a publish has the
/// engine locked moves the position onto the fold's manifest without waiting
/// for the publish, so a read during the publish starts from the fold's
/// manifest and sends no request to find it: its check sends one HEAD, for
/// the manifest after the fold's, and loads no manifest.
///
/// The fold runs on the session while the publish waits for its WAL put.
#[tokio::test]
async fn a_read_after_this_runtimes_own_fold_during_a_publish_starts_from_the_fold() {
    let fixture = fixture("own-fold-check", Parked::WalPuts).await;
    let session = fixture
        .writer
        .open_namespace(&fixture.namespace_id)
        .expect("open namespace");
    let replays = fixture.tail_replays();
    let manifest = |manifest_no| RecordedOperation::Head {
        key: metadata_manifest_object(&fixture.namespace_id, &manifest_no),
    };
    let before = loonfs_core::test_support::namespace_store(
        &(fixture.recording.clone() as SharedObjectStore),
        &fixture.namespace_id,
    )
    .load_current_manifest()
    .await
    .expect("current manifest")
    .manifest()
    .manifest_no;
    let fold_manifest = before.successor().expect("next manifest number");
    let after_fold = fold_manifest.successor().expect("next manifest number");
    let gate = fixture.recording.inner();
    gate.block_next();
    fixture.recording.reset();
    let (during, folded) = futures::join!(fixture.put("/during.txt"), async {
        gate.wait_until_blocked().await;
        let folded = bounded(NO_WAIT, session.fold_wal())
            .await
            .expect("the fold doesn't wait for the publish")
            .expect("fold");
        assert_eq!(folded.manifest_no, fold_manifest);
        fixture.clock.advance_ms(MAX_STALENESS_MS);
        let (positions, anchors) = fixture.head_sources();
        fixture.recording.reset();
        fixture.reader.stat("/small.txt").await.expect("stat");
        let operations = fixture.recording.take();
        let checked: Vec<_> = operations
            .iter()
            .filter(|operation| {
                operation
                    .key()
                    .starts_with(&metadata_manifest_prefix(&fixture.namespace_id))
            })
            .collect();
        assert!(
            checked.len() == 1 && *checked[0] == manifest(after_fold),
            "one HEAD finds nothing after the fold's manifest, and the check loads no \
             manifest: {operations:?}"
        );
        assert_eq!(
            fixture.head_sources(),
            (positions + 1, anchors),
            "the read starts from the position, without reloading"
        );
        gate.release();
        folded
    });
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let during = during.expect("the publish succeeds");
    assert_eq!(fixture.tail_replays(), replays);

    fixture.recording.reset();
    let entry = fixture.reader.stat("/during.txt").await.expect("stat");
    assert_eq!(entry.head_seq, during.committed_seq);
    assert_eq!(
        fixture.recording.take(),
        [],
        "the check confirmed the position on the fold's manifest"
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

/// Checks that a change of unknown effect drops the runtime's view of the
/// namespace, and that the next read loads the namespace by discovery, even
/// an eventual read within its maximum staleness.
#[tokio::test]
async fn a_change_of_unknown_effect_drops_the_view_and_the_next_read_loads_it() {
    let fixture = fixture("unknown-change", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    fixture.drop_position().await;
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    let entry = fixture
        .reader
        .stat("/next.txt")
        .await
        .expect("an eventual read after the drop");
    assert_eq!(entry.head_seq, commit.committed_seq);
    let operations = fixture.recording.take();
    assert!(
        matches!(
            operations.first(),
            Some(RecordedOperation::GetWithMetadata { key, .. })
                if *key == boundary(&fixture.namespace_id)
        ),
        "the read loads the namespace from its manifest boundary: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Checks that a fork keeps the runtime's view of the source. The fork folds
/// the source's tail through the runtime's fold path, which moves the
/// position onto the fold's manifest, and the snapshot records that the fork
/// writes aren't part of the view. So an eventual read after the fork starts
/// from the position without a request, and the source's next commit sends
/// its WAL put and nothing else.
#[tokio::test]
async fn a_fork_keeps_the_sources_position() {
    let fixture = fixture("fork-keeps-position", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    fixture
        .writer
        .fork_namespace(
            &fixture.namespace_id,
            &namespace_id("fork-keeps-position-fork"),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("fork");
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    let entry = fixture
        .reader
        .stat("/next.txt")
        .await
        .expect("an eventual read after the fork");
    assert_eq!(entry.head_seq, commit.committed_seq);
    let operations = fixture.recording.take();
    assert!(
        operations.is_empty(),
        "the read starts from the position without a request: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions + 1, anchors));
    fixture
        .put("/after.txt")
        .await
        .expect("a commit after the fork");
    let operations = fixture.recording.take();
    assert!(
        matches!(operations.as_slice(), [RecordedOperation::Put { .. }]),
        "the commit sends its WAL put and nothing else: {operations:?}"
    );
}

/// Checks that a refresh that started before a change of unknown effect,
/// and finishes after the runtime dropped its view for that change, installs
/// nothing, so the next read loads the namespace by discovery.
///
/// The test drops the position first, and a read loads the namespace into
/// the read cache. A strong read's refresh of that view then waits at its
/// WAL probe while a change of unknown effect drops the view. The refresh's
/// head is from before the change, so an eventual read after it would
/// otherwise start from that head without a request.
#[tokio::test]
async fn a_refresh_that_spans_a_change_of_unknown_effect_installs_nothing() {
    let fixture = fixture("spanning-refresh", Parked::WalReads).await;
    fixture.drop_position().await;
    fixture
        .reader
        .stat("/small.txt")
        .await
        .expect("load the view");
    let gate = fixture.recording.inner();
    gate.block_next();
    let strong = fixture.reader.strong();
    let (refreshed, ()) = futures::join!(strong.stat("/small.txt"), async {
        gate.wait_until_blocked().await;
        fixture.drop_position().await;
        gate.release();
    });
    refreshed.expect("the read whose refresh spans the fork");
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    fixture
        .reader
        .stat("/small.txt")
        .await
        .expect("an eventual read");
    let operations = fixture.recording.take();
    assert!(
        matches!(
            operations.first(),
            Some(RecordedOperation::GetWithMetadata { key, .. })
                if *key == boundary(&fixture.namespace_id)
        ),
        "the read loads the namespace from its manifest boundary: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Checks that a check of a writer position whose manifest's successors a
/// collection deleted, after a rival's claim, drops the position. The check's HEAD finds no
/// successor, but the manifest boundary is above the position's manifest, so
/// the read loads the newest manifest instead.
#[tokio::test]
async fn a_position_at_the_boundary_is_dropped_by_its_check() {
    let fixture = fixture("position-boundary", Parked::WalPuts).await;
    let store: SharedObjectStore = fixture.recording.clone();
    let namespace =
        || loonfs_core::test_support::namespace_store(store.clone(), &fixture.namespace_id);
    let position = namespace()
        .load_current_manifest()
        .await
        .expect("the position's manifest")
        .manifest()
        .manifest_no;
    // A rival claims and changes the retention window twice, and a
    // collection one grace window later deletes the manifests below the
    // newest change.
    rival_changes_the_window_twice(&fixture, "position-boundary-rival").await;
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .gc(&fixture.namespace_id)
        .await
        .expect("collect");

    fixture.clock.advance_ms(MAX_STALENESS_MS);
    let (positions, anchors) = fixture.head_sources();
    fixture.recording.reset();
    fixture.reader.stat("/small.txt").await.expect("stat");
    let operations = fixture.recording.take();
    let successor = metadata_manifest_object(
        &fixture.namespace_id,
        &position.successor().expect("next manifest number"),
    );
    assert!(
        matches!(
            operations.as_slice(),
            [RecordedOperation::Head { key }, RecordedOperation::GetWithMetadata { key: read, .. }, ..]
                if *key == successor && *read == boundary(&fixture.namespace_id)
        ),
        "the check reads the boundary after its HEAD: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Has a rival writer runtime claim the fixture's namespace and change its
/// retention window twice, so two manifests of the rival's epoch replace the
/// manifest of the fixture's writer position. The fixture's session isn't
/// told: it finds the claim at its next check or publication.
async fn rival_changes_the_window_twice(fixture: &Fixture, rival_name: &str) {
    let rival = peer(fixture, rival_name).await;
    let session = rival
        .open_namespace(&fixture.namespace_id)
        .expect("open the rival's session");
    for windows in [2, 3] {
        session
            .set_retention_window(Some(
                loonfs::RetentionWindow::new(windows * loonfs::GC_GRACE_WINDOW_MS)
                    .expect("a long enough window"),
            ))
            .await
            .expect("the rival claims and changes the window");
    }
    rival.drain().await.expect("drain the rival");
}

/// Returns a metadata cache that keeps no manifest, so a writer position's
/// manifest is read from the store each time.
fn no_manifests() -> MetadataCache {
    MetadataCache::builder().max_manifest_bytes(0).build()
}

/// Creates a fixture whose runtime keeps no manifest, in which a rival
/// claims the namespace and changes the retention window twice, and a
/// collection one grace window later deletes the manifest of the session's
/// position. Returns the fixture and
/// the number of the deleted manifest.
async fn collected_position(name: &str) -> (Fixture, ManifestNo) {
    use loonfs_objectstore::ObjectStore;
    let fixture = fixture_with_cache(name, Parked::WalPuts, no_manifests()).await;
    let store: SharedObjectStore = fixture.recording.clone();
    let namespace =
        || loonfs_core::test_support::namespace_store(store.clone(), &fixture.namespace_id);
    let position = namespace()
        .load_current_manifest()
        .await
        .expect("the position's manifest")
        .manifest()
        .manifest_no;
    rival_changes_the_window_twice(&fixture, &format!("{name}-rival")).await;
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .gc(&fixture.namespace_id)
        .await
        .expect("collect");
    let deleted = metadata_manifest_object(&fixture.namespace_id, &position);
    assert!(
        store.head(&deleted).await.expect("HEAD").is_none(),
        "the collection deletes the position's manifest"
    );
    (fixture, position)
}

/// Returns whether `operations` include a read of manifest `manifest_no`, or
/// a check of its successor, which a read from a position on that manifest
/// sends first.
fn uses_manifest(
    operations: &[RecordedOperation],
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> bool {
    let successor = metadata_manifest_object(
        namespace_id,
        &manifest_no.successor().expect("next manifest number"),
    );
    operations.iter().any(|operation| {
        is_manifest_get(operation, namespace_id, manifest_no)
            || matches!(operation, RecordedOperation::Head { key } if *key == successor)
    })
}

/// Returns the requests of a strong read's check of a writer position on
/// `position`, a manifest at or below the manifest boundary: a HEAD of the
/// manifest after `position`, and a read of the boundary, which drops the
/// position. Then come the requests of the discovery that finds the newest
/// manifest, `newest`.
fn collected_position_check(
    namespace_id: &NamespaceId,
    position: ManifestNo,
    newest: ManifestNo,
) -> Vec<String> {
    vec![
        format!(
            "head {namespace_id} MetadataManifest {:020}",
            position.0 + 1
        ),
        format!("get {namespace_id} Boundary"),
        format!("get {namespace_id} Boundary"),
        format!("list namespaces/{namespace_id}/manifests/"),
        format!("get {namespace_id} MetadataManifest {:020}", newest.0),
        format!("get {namespace_id} Boundary"),
    ]
}

/// Returns the number of the namespace's newest manifest in `fixture`'s
/// store.
async fn newest_manifest(fixture: &Fixture) -> ManifestNo {
    loonfs_core::test_support::namespace_store(
        fixture.recording.clone() as SharedObjectStore,
        &fixture.namespace_id,
    )
    .load_current_manifest()
    .await
    .expect("the newest manifest")
    .manifest()
    .manifest_no
}

/// Checks that the session's fold from a position whose manifest a
/// collection deleted, after a rival's claim, returns `writer_fenced` and
/// not a corrupt namespace. The position's manifest isn't in the segment
/// cache, so the fold reads the manifest from the store, finds nothing, and
/// reads the manifest boundary. The session then drops the position and
/// loads its view as a commit does, which finds the rival's claim. A new
/// session then folds from the newest manifest without a request for the
/// deleted manifest, and so does a strong read after it.
#[tokio::test]
async fn a_fold_from_a_collected_position_after_a_claim_returns_writer_fenced() {
    let name = "fold-collected";
    let (fixture, position) = collected_position(name).await;
    fixture.recording.reset();
    let error = fixture
        .namespace
        .fold_wal()
        .await
        .expect_err("the rival's claim fences the session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    let operations = fixture.recording.take();
    let namespace = fixture.namespace_id.to_string();
    let line = |request: &str| request.replace("{ns}", &namespace);
    assert_eq!(
        loonfs_test_support::stores::request_lines(&operations),
        [
            // The fold finds the position's manifest gone at or below the
            // boundary.
            format!("get {namespace} MetadataManifest {:020}", position.0),
            line("get {ns} Boundary"),
            // The writer's load finds the newest manifest, the rival's.
            line("get {ns} Boundary"),
            line("list namespaces/{ns}/manifests/"),
            line("get {ns} MetadataManifest 00000000000000000006"),
            line("get {ns} Boundary"),
        ]
    );
    fixture.close_session().await;
    fixture.recording.reset();
    fixture
        .writer
        .open_namespace(&fixture.namespace_id)
        .expect("open a new session")
        .fold_wal()
        .await
        .expect("a new session claims and folds from the newest manifest");
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
        "{operations:?}"
    );
    fixture
        .reader
        .strong()
        .stat("/small.txt")
        .await
        .expect("stat");
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
        "{operations:?}"
    );
}

/// Checks that a retention advance from a position whose manifest a
/// collection deleted, after a rival's claim, checks the position first, as
/// a strong read does, and that the check finds the manifest at or below the
/// manifest boundary and drops the position. The session then loads its
/// position again with the writer's load, as a fold does (see
/// [`a_fold_from_a_collected_position_after_a_claim_returns_writer_fenced`]),
/// which finds the rival's claim and returns `writer_fenced`, so the advance
/// doesn't return a result computed from the rival's manifest. A strong
/// read after it doesn't start from the deleted manifest.
#[tokio::test]
async fn a_retention_advance_from_a_collected_position_after_a_claim_returns_writer_fenced() {
    let name = "retention-collected";
    let (fixture, position) = collected_position(name).await;
    let newest = newest_manifest(&fixture).await;
    fixture.recording.reset();
    let error = fixture
        .namespace
        .advance_retention_floor()
        .await
        .expect_err("the rival's claim fences the session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    let operations = fixture.recording.take();
    assert_eq!(
        loonfs_test_support::stores::request_lines(&operations),
        collected_position_check(&fixture.namespace_id, position, newest)
    );
    fixture
        .reader
        .strong()
        .stat("/small.txt")
        .await
        .expect("stat");
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
        "{operations:?}"
    );
}

/// Checks that a retention window change from a position whose manifest a
/// collection deleted, after a rival's claim, returns `writer_fenced`
/// instead of publishing over the rival's manifests: the change finds the
/// position's manifest gone at or below the boundary, and the session loads
/// its position again with the writer's load, which finds the rival's
/// claim. The window stays the rival's.
#[tokio::test]
async fn a_window_change_from_a_collected_position_after_a_claim_returns_writer_fenced() {
    let name = "window-collected";
    let (fixture, _) = collected_position(name).await;
    let error = fixture
        .namespace
        .set_retention_window(None)
        .await
        .expect_err("the rival's claim fences the session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    let store: SharedObjectStore = fixture.recording.clone();
    let window = loonfs_core::test_support::namespace_store(store, &fixture.namespace_id)
        .load_current_manifest()
        .await
        .expect("the current manifest")
        .payload()
        .retention_window_ms
        .map(std::num::NonZeroU64::get);
    assert_eq!(window, Some(3 * loonfs::GC_GRACE_WINDOW_MS));
}

/// Checks that a fold from a position whose manifest is missing while the
/// manifest is above the manifest boundary reports a corrupt namespace.
#[tokio::test]
async fn a_missing_manifest_above_the_boundary_is_corrupt() {
    use loonfs_objectstore::ObjectStore;
    let name = "missing-above-boundary";
    let fixture = fixture_with_cache(name, Parked::WalPuts, no_manifests()).await;
    let store: SharedObjectStore = fixture.recording.clone();
    let position = loonfs_core::test_support::namespace_store(store.clone(), &fixture.namespace_id)
        .load_current_manifest()
        .await
        .expect("the position's manifest")
        .manifest()
        .manifest_no;
    store
        .delete(&metadata_manifest_object(&fixture.namespace_id, &position))
        .await
        .expect("delete the position's manifest");
    let error = fixture
        .namespace
        .fold_wal()
        .await
        .expect_err("a missing manifest above the boundary");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}

/// Checks that an eventual read within its maximum staleness from a position
/// whose manifest a collection deleted returns the file from the newest
/// manifest. The read asks the store for the position's manifest, finds
/// nothing, and then reads a newer manifest.
#[tokio::test]
async fn a_read_from_a_collected_position_reads_the_newest_manifest() {
    let (fixture, position) = collected_position("read-collected").await;
    let newest = loonfs_core::test_support::namespace_store(
        fixture.recording.clone() as SharedObjectStore,
        &fixture.namespace_id,
    )
    .load_current_manifest()
    .await
    .expect("the newest manifest")
    .manifest()
    .manifest_no;
    fixture.recording.reset();
    let file = fixture
        .reader
        .read_file("/small.txt")
        .await
        .expect("the read retries at the newest manifest");
    assert_eq!(file.bytes, [b's'; 1024]);
    let operations = fixture.recording.take();
    let missing = operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                RecordedOperation::Get { key, result_bytes: 0, .. }
                    if *key == metadata_manifest_object(&fixture.namespace_id, &position)
            )
        })
        .unwrap_or_else(|| panic!("a GET of the deleted manifest: {operations:?}"));
    assert!(
        operations[missing..]
            .iter()
            .any(|operation| is_manifest_get(operation, &fixture.namespace_id, newest)),
        "a read of the newest manifest after the GET that found nothing: {operations:?}"
    );
}

/// Checks that the reads that need a check of the position share the
/// session's checks, and that the check confirms the position for the
/// eventual reads after it.
///
/// A strong read starts a check, which the store parks. Strong reads that
/// arrive during that check wait for it, because it started before they
/// did, and then share the next one. An eventual read past its maximum
/// staleness that arrives during the check waits for it too, and takes the
/// position that it confirmed. So the wave sends two HEADs, and an eventual
/// read after it sends nothing.
#[tokio::test]
async fn a_wave_of_reads_shares_at_most_two_checks() {
    let fixture = fixture("shared-check", Parked::ManifestChecks).await;
    fixture.clock.advance_ms(MAX_STALENESS_MS);
    let gate = fixture.recording.inner();
    gate.block_next();
    fixture.recording.reset();
    let strong = fixture.reader.strong();
    let first = strong.stat("/small.txt");
    let (first, ()) = futures::join!(first, async {
        gate.wait_until_blocked().await;
        let readers = [
            strong.clone(),
            strong.clone(),
            strong.clone(),
            fixture.reader.clone(),
        ];
        let mut wave = pin!(futures::future::join_all(
            readers.iter().map(|reader| reader.stat("/small.txt"))
        ));
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

/// Returns a fixture whose writer session keeps a position on a manifest
/// that a collection deleted, with the segments that only that manifest
/// lists, and whose runtime caches no segment's rows, so every read of a
/// segment goes to the store. With `cached`, the
/// runtime's segment cache still has the deleted manifest, which the
/// writer's own fold put there. The fixture also returns the bytes of the
/// deleted manifest.
///
/// The writer folds, so its position is on the fold's manifest. A rival
/// writer runtime then claims the namespace, commits and folds eight times,
/// and compacts while a unit is due, which rewrites the position's runs. A collection one grace window
/// later raises the boundary over the position's manifest and deletes the
/// manifest and the segments that only it lists. The position isn't checked
/// in between, because nothing in the writer's runtime reads.
///
/// `history` chooses what the namespace has besides the fixture's files.
/// For a variant that adds rows, the fixture checks, by listing the store,
/// that the collection deleted the position's segments of that row family.
async fn position_on_a_collected_manifest(
    name: &str,
    cached: bool,
    history: History,
) -> (Fixture, ManifestNo, bytes::Bytes) {
    use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
    let cache = MetadataCache::builder().max_segment_bytes(0);
    let cache = if cached {
        cache
    } else {
        cache.max_manifest_bytes(0)
    };
    let fixture = fixture_with_cache(name, Parked::WalPuts, cache.build()).await;
    let store: SharedObjectStore = fixture.recording.clone();
    let namespace_id = fixture.namespace_id.clone();
    let actor = loonfs_test_support::test_actor();
    if let History::Merged = history {
        let fork = loonfs_test_support::ids::namespace_id(&format!("{name}-fork"));
        fixture
            .writer
            .fork_namespace(&namespace_id, &fork, &actor)
            .await
            .expect("fork");
        fixture
            .writer
            .open_namespace(&fork)
            .expect("open the fork")
            .put_file("/forked.txt", b"fork", &actor)
            .await
            .expect("a commit in the fork");
        fixture
            .namespace
            .merge_fork(&fork, &actor)
            .await
            .expect("merge the fork");
    }
    if let History::Administered = history {
        let grants = loonfs_types::AccessGrants::new(std::collections::BTreeMap::from([(
            loonfs_types::PrincipalId::parse(ADMINISTRATOR).expect("principal"),
            loonfs_types::AccessRight::Admin.into(),
        )]))
        .expect("grants");
        loonfs::test_support::set_root_access(&fixture.writer, &namespace_id, grants).await;
        fixture.namespace.create_snapshot().await.expect("snapshot");
        fixture.put("/after-snapshot.txt").await.expect("put");
    }
    fixture
        .namespace
        .fold_wal()
        .await
        .expect("fold the writer's tail");
    fixture.writer.drain().await.expect("drain");
    let position = loonfs_core::test_support::namespace_store(store.clone(), &namespace_id)
        .load_current_manifest()
        .await
        .expect("the position's manifest");
    let key = metadata_manifest_object(&namespace_id, &position.manifest().manifest_no);
    let manifest_bytes = store
        .get(&key, None)
        .await
        .expect("GET")
        .expect("the position's manifest");
    if let History::AppendInTail = history {
        fixture
            .namespace
            .append_file("/small.txt", b"more", &actor)
            .await
            .expect("append");
    }
    let rival = peer(&fixture, &format!("{name}-rival")).await;
    let rival_session = rival
        .open_namespace(&namespace_id)
        .expect("open the rival's session");
    for index in 0..8 {
        rival_session
            .put_file(&format!("/{index}.txt"), b"file", &actor)
            .await
            .expect("the rival claims and commits");
        rival_session.fold_wal().await.expect("the rival folds");
    }
    rival_session
        .maintain_metadata_while_due(&loonfs::MaintenanceCancellation::default())
        .await
        .expect("the rival compacts");
    rival.drain().await.expect("drain the rival");
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .gc(&namespace_id)
        .await
        .expect("collect");
    assert!(
        store.head(&key).await.expect("HEAD").is_none(),
        "the collection deletes the position's manifest"
    );
    let mut lost = 0;
    for segment in position
        .payload()
        .runs
        .iter()
        .flat_map(|run| run.segments.iter())
    {
        let key = loonfs_objectstore::keys::metadata_segment_object_key(segment);
        lost += usize::from(store.head(&key).await.expect("HEAD").is_none());
    }
    assert!(lost > 0, "the collection deletes a segment of the position");
    let family = match history {
        History::Files | History::Administered => None,
        History::AppendInTail => Some(MetadataRowFamily::ContentLayouts),
        History::Merged => Some(MetadataRowFamily::Merges),
    };
    if let Some(family) = family {
        let listed = store
            .list_prefix(&loonfs_objectstore::keys::metadata_segment_prefix(
                &namespace_id,
            ))
            .await
            .expect("list the segments");
        let needed: Vec<String> = position
            .payload()
            .runs
            .iter()
            .flat_map(|run| run.segments.iter())
            .filter(|segment| segment.family == family)
            .map(loonfs_objectstore::keys::metadata_segment_object_key)
            .collect();
        assert!(
            !needed.is_empty(),
            "the position lists a {family:?} segment"
        );
        assert!(
            needed.iter().all(|key| !listed.contains(key)),
            "the collection deletes the position's {family:?} segments: {needed:?}"
        );
    }
    (fixture, position.manifest().manifest_no, manifest_bytes)
}

/// What a namespace from [`position_on_a_collected_manifest`] has besides
/// the fixture's files.
enum History {
    /// Nothing else.
    Files,
    /// The writer's tail appends to `/small.txt`, whose first bytes the
    /// position's manifest lays out in a segment that only that manifest
    /// lists.
    AppendInTail,
    /// The writer merged a fork before its fold, so the position's manifest
    /// indexes that merge.
    Merged,
    /// The root's access row makes [`ADMINISTRATOR`] an administrator, and
    /// the namespace has a snapshot of a manifest before the position's.
    Administered,
}

/// The principal that a namespace from [`History::Administered`] has as its
/// administrator.
const ADMINISTRATOR: &str = "prn_root";

/// Checks that a retention advance from a writer position whose manifest a
/// collection deleted after a rival's claim returns `writer_fenced`, whether
/// or not the runtime's segment cache still has the deleted manifest, and
/// sends no request for that manifest.
///
/// The advance first checks the position as a strong read does. The check
/// finds the position's manifest at or below the manifest boundary and drops
/// the position, so in both cases the session loads its position again with
/// the writer's load, which finds the rival's claim by discovery, as the
/// sweep's fold and the snapshot do (see
/// [`a_sweep_and_a_snapshot_from_a_position_on_a_collected_manifest_are_fenced`]).
#[tokio::test]
async fn a_retention_advance_from_a_position_on_a_collected_manifest_is_fenced() {
    for cached in [false, true] {
        let name = format!("collected-advance-{}", if cached { "hit" } else { "miss" });
        let (fixture, position, _) =
            position_on_a_collected_manifest(&name, cached, History::Files).await;
        let newest = newest_manifest(&fixture).await;
        fixture.recording.reset();
        let error = fixture
            .namespace
            .advance_retention_floor()
            .await
            .expect_err("the rival's claim fences the session");
        assert_eq!(
            error.code(),
            ErrorCode::WriterFenced,
            "cached={cached}: {error:?}"
        );
        assert_eq!(
            loonfs_test_support::stores::request_lines(&fixture.recording.take()),
            collected_position_check(&fixture.namespace_id, position, newest),
            "cached={cached}"
        );
    }
}

/// Checks that a writer whose position is on a manifest that a collection
/// deleted after a rival's claim, with an append in the tail that the
/// rival's fold covered, loses its position without a corrupt namespace: its
/// next commit returns `writer_fenced`, and a new session commits, folds and
/// reads the appended file whole from the newest manifest.
#[tokio::test]
async fn a_fenced_writer_on_a_collected_manifest_commits_and_folds_in_a_new_session() {
    let name = "collected-append-fold";
    let (fixture, _, _) = position_on_a_collected_manifest(name, true, History::AppendInTail).await;
    let fenced = fixture
        .put("/before.txt")
        .await
        .expect_err("the rival's claim fences the session");
    assert_eq!(fenced.code(), ErrorCode::WriterFenced, "{fenced:?}");
    fixture.close_session().await;
    let session = fixture
        .writer
        .open_namespace(&fixture.namespace_id)
        .expect("open a new session");
    session
        .put_file("/after.txt", b"file", &loonfs_test_support::test_actor())
        .await
        .expect("the new session claims and commits");
    assert_eq!(
        session
            .fold_wal()
            .await
            .expect("the new session folds")
            .outcome,
        FoldWalOutcome::Published
    );
    let file = fixture
        .reader
        .read_file("/small.txt")
        .await
        .expect("read the appended file");
    assert_eq!(file.bytes, [&[b's'; 1024][..], b"more"].concat());
}

/// Checks that a fold whose tail appends to a file returns
/// `namespace_corrupt` when the file's layout segment is missing from the
/// current manifest, which no newer manifest replaced.
#[tokio::test]
async fn a_fold_that_finds_a_segment_of_the_current_manifest_missing_is_corrupt() {
    use loonfs_objectstore::ObjectStore;
    let name = "current-append-fold";
    let cache = MetadataCache::builder().max_segment_bytes(0).build();
    let fixture = fixture_with_cache(name, Parked::WalPuts, cache).await;
    let store: SharedObjectStore = fixture.recording.clone();
    fixture.namespace.fold_wal().await.expect("fold the file");
    let current = loonfs_core::test_support::namespace_store(store.clone(), &fixture.namespace_id)
        .load_current_manifest()
        .await
        .expect("the current manifest");
    fixture
        .namespace
        .append_file("/small.txt", b"more", &loonfs_test_support::test_actor())
        .await
        .expect("append");
    for segment in current
        .payload()
        .runs
        .iter()
        .flat_map(|run| run.segments.iter())
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
    {
        store
            .delete(&loonfs_objectstore::keys::metadata_segment_object_key(
                segment,
            ))
            .await
            .expect("delete the layout segment");
    }
    let error = fixture
        .namespace
        .fold_wal()
        .await
        .expect_err("a missing segment of the current manifest");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error:?}");
    assert!(format!("{error:?}").contains("MissingSegment"), "{error:?}");
}

/// Checks that the sweep's fold, which a writer runtime's host runs once a
/// fold is due (here at a one-object threshold), and a user snapshot,
/// which folds first, return `writer_fenced` from the state of
/// [`a_fenced_writer_on_a_collected_manifest_commits_and_folds_in_a_new_session`],
/// where a rival claimed the namespace. Each fold builds on the position's
/// manifest, which the segment cache still has, and loses its put to a
/// manifest above the position, so the session drops the position and the
/// writer's load finds the rival's claim, instead of answering from the
/// rival's manifest.
#[tokio::test]
async fn a_sweep_and_a_snapshot_from_a_position_on_a_collected_manifest_are_fenced() {
    for snapshot in [false, true] {
        let name = if snapshot {
            "collected-append-snapshot"
        } else {
            "collected-append-sweep"
        };
        let (fixture, _, _) =
            position_on_a_collected_manifest(name, true, History::AppendInTail).await;
        let error = if snapshot {
            fixture.namespace.create_snapshot().await.map(|_| ())
        } else {
            fixture
                .namespace
                .maintain_metadata_with_options(&loonfs::MaintainMetadataOptions {
                    max_wal_tail_objects: std::num::NonZeroU64::MIN,
                })
                .await
                .map(|_| ())
        }
        .expect_err("the rival's claim fences the session");
        assert_eq!(error.code(), ErrorCode::WriterFenced, "{name}: {error:?}");
    }
}

/// Checks that a read of the namespace's metadata and a lookup of an
/// extension's entry, as grep looks up its own, succeed from a writer
/// position whose manifest a collection deleted when neither the segment
/// cache nor the store has the manifest. Each finds the manifest missing at
/// or below the manifest boundary, returns `ManifestReplaced`, and runs again
/// from the newest manifest.
#[tokio::test]
async fn reads_of_the_manifest_alone_from_a_collected_position_succeed() {
    for metadata in [true, false] {
        let name = if metadata {
            "collected-metadata"
        } else {
            "collected-extension-entry"
        };
        let (fixture, _, _) = position_on_a_collected_manifest(name, false, History::Files).await;
        if metadata {
            fixture
                .reader
                .head_metadata()
                .await
                .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
        } else {
            let entry = fixture
                .reader
                .read_through_views(|| async {
                    let view = fixture.reader.read_view().await?;
                    view.find_extension_entry(
                        &loonfs_types::ExtensionName::parse("grep").expect("extension name"),
                    )
                    .await
                })
                .await
                .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
            assert_eq!(entry, None);
        }
    }
}

/// Checks that an eventual read of the next merge range succeeds when the
/// writer position's manifest was collected, the runtime's segment
/// cache still has that manifest, and every segment of the namespace's merge
/// index that the manifest lists is gone. The read finds a segment of the
/// position's manifest missing, the manifest isn't current, and the read
/// runs again from the newest manifest.
#[tokio::test]
async fn a_next_merge_range_from_a_position_on_a_collected_manifest_succeeds() {
    let name = "collected-merge-range";
    let (fixture, _, _) = position_on_a_collected_manifest(name, true, History::Merged).await;
    fixture
        .namespace
        .next_merge_range(&namespace_id(&format!("{name}-fork")))
        .await
        .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
}

/// Checks that an eventual read of the next merge range succeeds when the
/// writer session's position in the fork is on a manifest that a collection
/// deleted, the runtime caches no manifest, and the namespace's own position
/// is current. The read finds the fork's manifest missing at or below the
/// fork's manifest boundary, the loop drops the runtime's view of the fork,
/// and the read runs again from the fork's newest manifest.
#[tokio::test]
async fn a_next_merge_range_from_a_fork_position_on_a_collected_manifest_succeeds() {
    use loonfs_objectstore::ObjectStore;
    let name = "collected-fork-range";
    let cache = MetadataCache::builder()
        .max_segment_bytes(0)
        .max_manifest_bytes(0)
        .build();
    let fixture = fixture_with_cache(name, Parked::WalPuts, cache).await;
    let store: SharedObjectStore = fixture.recording.clone();
    let actor = loonfs_test_support::test_actor();
    let fork = namespace_id(&format!("{name}-fork"));
    fixture
        .writer
        .fork_namespace(&fixture.namespace_id, &fork, &actor)
        .await
        .expect("fork");
    let fork_namespace = fixture.writer.open_namespace(&fork).expect("open the fork");
    fork_namespace
        .put_file("/forked.txt", b"fork", &actor)
        .await
        .expect("a commit in the fork");
    fork_namespace.fold_wal().await.expect("fold the fork");
    fixture.writer.drain().await.expect("drain");
    let position = loonfs_core::test_support::namespace_store(store.clone(), &fork)
        .load_current_manifest()
        .await
        .expect("the fork position's manifest");
    let rival = peer(&fixture, &format!("{name}-rival")).await;
    let rival_fork = rival
        .open_namespace(&fork)
        .expect("open the rival's session");
    for index in 0..8 {
        rival_fork
            .put_file(&format!("/{index}.txt"), b"file", &actor)
            .await
            .expect("the rival claims the fork and commits");
        rival_fork.fold_wal().await.expect("the rival folds");
    }
    rival_fork
        .maintain_metadata_while_due(&loonfs::MaintenanceCancellation::default())
        .await
        .expect("the rival compacts");
    rival.drain().await.expect("drain the rival");
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .gc(&fork)
        .await
        .expect("collect");
    let key = metadata_manifest_object(&fork, &position.manifest().manifest_no);
    assert!(
        store.head(&key).await.expect("HEAD").is_none(),
        "the collection deletes the fork position's manifest"
    );
    fixture
        .namespace
        .next_merge_range(&fork)
        .await
        .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
}

/// Checks that a rebase creates its new fork when the writer position's
/// manifest was collected, the runtime's segment cache still has that
/// manifest, and every segment of the namespace's merge index that the
/// manifest lists is gone.
///
/// The rebase reads the fork point at a `strong` head. With the writer
/// session open, the runtime checks the position first and moves it onto the
/// newest manifest. With the session closed, the position becomes the cached
/// head, whose manifest check isn't due, so the read finds a segment of the
/// collected manifest missing and runs again from the newest manifest.
#[tokio::test]
async fn a_rebase_from_a_position_on_a_collected_manifest_succeeds() {
    for session_open in [true, false] {
        let name = if session_open {
            "collected-rebase"
        } else {
            "collected-rebase-head"
        };
        let (fixture, _, _) = position_on_a_collected_manifest(name, true, History::Merged).await;
        if !session_open {
            fixture.close_session().await;
        }
        fixture
            .writer
            .fork_namespace_for_rebase(
                &fixture.namespace_id,
                &namespace_id(&format!("{name}-fork")),
                &namespace_id(&format!("{name}-rebase")),
                &loonfs_test_support::test_actor(),
            )
            .await
            .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
    }
}

/// Checks that a retention advance from a writer position whose manifest is
/// at or below the manifest boundary, still in the store, and missing
/// segments that a collection deleted, after a rival's claim, returns
/// `writer_fenced` when the runtime's segment cache doesn't have the
/// manifest.
///
/// The fixture's collection deletes the manifest, and the test puts the
/// manifest back by hand. The advance first checks the position as a strong
/// read does. The check finds the manifest at or below the manifest boundary
/// and drops the position, so the advance doesn't read the stored manifest,
/// and the writer's load that follows finds the rival's claim by discovery.
#[tokio::test]
async fn a_retention_advance_from_a_stored_position_manifest_below_the_boundary_is_fenced() {
    use loonfs_objectstore::ObjectStore;
    let name = "stored-below-boundary";
    let (fixture, position, manifest_bytes) =
        position_on_a_collected_manifest(name, false, History::Files).await;
    let newest = newest_manifest(&fixture).await;
    let store: SharedObjectStore = fixture.recording.clone();
    store
        .put(
            &metadata_manifest_object(&fixture.namespace_id, &position),
            manifest_bytes,
            loonfs_objectstore::PutMode::CreateIfAbsent,
        )
        .await
        .expect("put the position's manifest back");
    fixture.recording.reset();
    let error = fixture
        .namespace
        .advance_retention_floor()
        .await
        .expect_err("the rival's claim fences the session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.recording.take()),
        collected_position_check(&fixture.namespace_id, position, newest)
    );
}

/// Checks that a strong read whose cached head is on a manifest that a
/// collection deleted after a rival's claim reads from the newest manifest,
/// and checks the read's requests.
///
/// The writer's session closes, so its position, on the deleted manifest,
/// becomes the cached head. The strong read's WAL probe finds the fence of
/// the rival's claim after the cached head's tip, so the read drops the
/// cached head, loads the head by discovery and reads from the newest
/// manifest, without a request for the deleted manifest or its segments.
#[tokio::test]
async fn a_strong_read_from_a_cached_head_below_a_claim_reads_the_newest_manifest() {
    let name = "collected-strong-read";
    let (fixture, _, _) = position_on_a_collected_manifest(name, true, History::Files).await;
    fixture
        .writer
        .open_namespace(&fixture.namespace_id)
        .expect("a handle on the session")
        .close()
        .await
        .expect("close the session");
    fixture.recording.reset();
    let file = fixture
        .reader
        .strong()
        .read_file("/small.txt")
        .await
        .expect("the read runs again from the newest manifest");
    assert_eq!(file.bytes, [b's'; 1024]);
    let namespace = fixture.namespace_id.to_string();
    let line = |request: &str| request.replace("{ns}", &namespace);
    let expected = [
        // The WAL probe of the cached head finds the rival's claim above it.
        line("get {ns} WalObject 00000000000000000003"),
        // The read loads the head by discovery, with the WAL read above the
        // newest manifest and the check of its successor.
        line("get {ns} Boundary"),
        line("list namespaces/{ns}/manifests/"),
        line("get {ns} MetadataManifest 00000000000000000018"),
        line("get {ns} Boundary"),
        line("get {ns} WalObject 00000000000000000012"),
        line("head {ns} MetadataManifest 00000000000000000019"),
        line("get {ns} Boundary"),
        // The read from the newest manifest.
        line("get {ns} MetadataSegment"),
        line("get {ns} MetadataSegment"),
        line("get {ns} MetadataSegment"),
        line("get {ns} MetadataSegment"),
        "get pool ContentBlob".to_owned(),
    ];
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.recording.take()),
        expected
    );
}

/// Checks that the reads of a subject's administrator rights succeed
/// from a cached head whose manifest a collection deleted, when neither the
/// segment cache nor the store has the manifest: a deletion, a lookup of a
/// snapshot, and a listing of a snapshot's files and a stat through
/// `read_at_snapshot`, which an HTTP read of a snapshot runs. Those two run
/// again under the call's one read slot.
///
/// The writer session closes, so its position becomes the cached head. Each
/// check reads the root's access row at a `strong` head, whose manifest check
/// isn't due, finds the manifest missing at or below the manifest boundary,
/// and runs again from the newest manifest.
#[tokio::test]
async fn administrator_checks_from_a_collected_position_succeed() {
    let subject = loonfs_types::Subject {
        principals: loonfs_types::PrincipalSet::new(std::collections::BTreeSet::from([
            loonfs_types::PrincipalId::parse(ADMINISTRATOR).expect("principal"),
        ]))
        .expect("principals"),
    };
    for case in ["delete", "get-snapshot", "snapshot-files", "snapshot-stat"] {
        let name = format!("collected-{case}");
        let (fixture, _, _) =
            position_on_a_collected_manifest(&name, false, History::Administered).await;
        fixture.close_session().await;
        let snapshot_id = fixture
            .reader
            .list_snapshots()
            .page()
            .await
            .expect("list the snapshots")
            .snapshots
            .remove(0)
            .snapshot_id;
        let context = loonfs::Context::from(subject.clone());
        let reader = fixture.reader.with_context(context.clone());
        let result = match case {
            "delete" => fixture
                .writer
                .with_context(context)
                .open_namespace(&fixture.namespace_id)
                .expect("open the namespace")
                .delete()
                .await
                .map(drop),
            "get-snapshot" => reader.get_snapshot(&snapshot_id).await.map(drop),
            "snapshot-files" => reader
                .read_at_snapshot(&snapshot_id, |view| async move {
                    view.list_files().page().await
                })
                .await
                .map(drop),
            _ => reader
                .read_at_snapshot(
                    &snapshot_id,
                    |view| async move { view.stat("/small.txt").await },
                )
                .await
                .map(drop),
        };
        result.unwrap_or_else(|error| panic!("{case}: {:?} {error:?}", error.code()));
    }
}

/// Checks that a strong read in a read-only runtime whose cached head is on
/// a manifest that the writer's compaction replaced and a collection
/// deleted, with no fence above the head, reads again from the newest
/// manifest, and checks the read's requests.
///
/// The reader caches the head after the writer's fold. The writer then
/// commits and folds eight times and compacts while a unit is due, which
/// rewrites the cached manifest's runs, and a collection one grace window
/// later deletes that manifest and the segments that only it lists. The
/// strong read probes the WAL above the cached head, and the manifest check
/// isn't due. The read then finds a segment of the
/// deleted manifest missing, finds the newer current manifest, and drops the
/// runtime's view of the namespace. The second run loads the head by
/// discovery and reads from the newest manifest.
#[tokio::test]
async fn a_strong_read_from_a_collected_manifest_reads_the_newest_manifest() {
    use loonfs_objectstore::ObjectStore;
    let name = "collected-strong-read-only";
    let cache = MetadataCache::builder().max_segment_bytes(0).build();
    let fixture = fixture_with_cache(name, Parked::WalPuts, cache).await;
    let store: SharedObjectStore = fixture.recording.clone();
    fixture.namespace.fold_wal().await.expect("fold the file");
    let reader = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .read_only()
        .metadata_cache(MetadataCache::builder().max_segment_bytes(0).build())
        .build()
        .await
        .expect("reader");
    let reads = reader.namespace(&fixture.namespace_id);
    reads
        .strong()
        .stat("/small.txt")
        .await
        .expect("the reader caches the head");
    let cached = newest_manifest(&fixture).await;
    for index in 0..8 {
        fixture.put(&format!("/{index}.txt")).await.expect("put");
        fixture.namespace.fold_wal().await.expect("fold");
    }
    fixture
        .namespace
        .maintain_metadata_while_due(&loonfs::MaintenanceCancellation::default())
        .await
        .expect("compact");
    fixture.writer.drain().await.expect("drain");
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .gc(&fixture.namespace_id)
        .await
        .expect("collect");
    assert!(
        store
            .head(&metadata_manifest_object(&fixture.namespace_id, &cached))
            .await
            .expect("HEAD")
            .is_none(),
        "the collection deletes the cached head's manifest"
    );
    fixture.recording.reset();
    let file = reads
        .strong()
        .read_file("/small.txt")
        .await
        .expect("the read runs again from the newest manifest");
    assert_eq!(file.bytes, [b's'; 1024]);
    let namespace = fixture.namespace_id.to_string();
    let line = |request: &str| request.replace("{ns}", &namespace);
    let expected = [
        // The first run: the WAL probe of the cached head, and the read of a
        // segment that the collection deleted.
        line("get {ns} WalObject 00000000000000000003"),
        line("get {ns} MetadataSegment"),
        // The check that a newer manifest replaced the read's manifest.
        line("get {ns} Boundary"),
        line("list namespaces/{ns}/manifests/"),
        line("get {ns} MetadataManifest 00000000000000000017"),
        line("get {ns} Boundary"),
        // The second run's load of the head by discovery, with the WAL read
        // above the newest manifest and the check of its successor.
        line("get {ns} Boundary"),
        line("list namespaces/{ns}/manifests/"),
        line("get {ns} Boundary"),
        line("get {ns} WalObject 00000000000000000011"),
        line("head {ns} MetadataManifest 00000000000000000018"),
        line("get {ns} Boundary"),
        // The second run's read from the newest manifest.
        line("get {ns} MetadataSegment"),
        line("get {ns} MetadataSegment"),
        line("get {ns} MetadataSegment"),
        line("get {ns} MetadataSegment"),
        "get pool ContentBlob".to_owned(),
    ];
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.recording.take()),
        expected
    );
}

/// Checks that a retention window change and an extension's entry that the
/// session publishes while its commit waits at its WAL put don't wait for
/// the commit: each puts its manifest from the session's position without
/// the session's engine, and the commit then lands in the tail above them.
#[tokio::test]
async fn a_window_change_and_an_extension_entry_during_a_commits_put_do_not_wait_for_it() {
    let fixture = fixture("publications-beside-a-commit", Parked::WalPuts).await;
    let gate = fixture.recording.inner();
    gate.block_next();
    let (during, ()) = futures::join!(fixture.put("/during.txt"), async {
        gate.wait_until_blocked().await;
        bounded(
            NO_WAIT,
            fixture.namespace.set_retention_window(Some(
                loonfs::RetentionWindow::new(2 * loonfs::GC_GRACE_WINDOW_MS)
                    .expect("a long enough window"),
            )),
        )
        .await
        .expect("the window change doesn't wait for the commit")
        .expect("the window change");
        let basis = fixture
            .namespace
            .load_extension_basis()
            .await
            .expect("the extension's basis");
        let published = bounded(
            NO_WAIT,
            fixture.namespace.publish_extension(
                &basis,
                &loonfs_types::ExtensionName::parse("grep").expect("extension name"),
                loonfs_types::format::manifest::ExtensionEntry::new(
                    1,
                    Vec::new(),
                    serde_json::Map::new(),
                ),
            ),
        )
        .await
        .expect("the extension's entry doesn't wait for the commit")
        .expect("the extension's entry");
        assert!(matches!(
            published,
            loonfs::engine::ExtensionPublication::Published { .. }
        ));
        gate.release();
    });
    let during = during.expect("the commit");
    let entry = fixture
        .reader
        .strong()
        .stat("/during.txt")
        .await
        .expect("the commit is in the tail above both manifests");
    assert_eq!(entry.head_seq, during.committed_seq);
}
