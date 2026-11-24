//! Reads in a runtime with the namespace's writer session, which start from
//! the session's position instead of the read cache.

use crate::common::{is_manifest_head, wait_for_operation};
use loonfs::metrics::DefaultMetricsRecorder;
use loonfs::test_support::{counter, writer};
use loonfs::{
    Commit, Consistency, ErrorCode, FoldWalOutcome, LoonFs, MetadataCache, Namespace, ReadAt,
    ReadFileOptions, ReadOnly, SharedObjectStore, StatOptions, Writable,
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

/// Returns the options of a strong stat.
fn strong() -> StatOptions {
    StatOptions {
        at: ReadAt::Live(Some(Consistency::Strong)),
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
        .stat_with_options("/next.txt", &strong())
        .await
        .expect("stat");
    assert_eq!(entry.head_seq, commit.committed_seq);
    let file = fixture
        .reader
        .read_file_with_options(
            "/small.txt",
            &ReadFileOptions {
                at: ReadAt::Live(Some(Consistency::Strong)),
            },
        )
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
/// read reloads the namespace from its manifest boundary.
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
        .position(|operation| operation.key() == boundary(&fixture.namespace_id))
        .expect("a reload from the manifest boundary");
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
                .all(|operation| !matches!(operation, RecordedOperation::List { .. })),
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
/// while a publish has the engine locked, loads the fold's manifest from the
/// segment cache without a request, finds nothing after it, waits for the
/// publish to release the engine, and then moves the position onto the
/// fold's manifest without reloading, so reads start from the fold's
/// manifest.
///
/// The fold runs through maintenance while the publish waits for its WAL
/// put, so the fold's own move of the position waits for the engine too.
#[tokio::test]
async fn a_check_that_finds_this_runtimes_own_fold_keeps_the_position() {
    let fixture = fixture("own-fold-check", Parked::WalPuts).await;
    let maintenance = fixture.writer.maintenance(writer_id("own-fold-check"));
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
        let (folded, ()) = futures::join!(maintenance.fold_wal(&fixture.namespace_id), async {
            // The fold's manifest put is its last request before its move
            // of the position, which waits for the publish's engine.
            wait_for_operation(&fixture.recording, |operation| {
                matches!(
                    operation,
                    RecordedOperation::Put { key, .. }
                        if *key == metadata_manifest_object(&fixture.namespace_id, &fold_manifest)
                )
            })
            .await;
            fixture.clock.advance_ms(MAX_STALENESS_MS);
            let (positions, anchors) = fixture.head_sources();
            fixture.recording.reset();
            let (entry, ()) = futures::join!(fixture.reader.stat("/small.txt"), async {
                // The walk ends at the missing manifest after the fold's, and
                // the check's move waits for the publish's engine.
                wait_for_operation(&fixture.recording, |operation| {
                    is_manifest_get(operation, &fixture.namespace_id, after_fold)
                })
                .await;
                gate.release();
            });
            entry.expect("stat");
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
                checked.len() == 2
                    && *checked[0] == manifest(fold_manifest)
                    && is_manifest_get(checked[1], &fixture.namespace_id, after_fold),
                "one HEAD finds the fold's manifest, the check loads the fold's manifest \
                 from the segment cache and finds nothing after it, and the read starts \
                 from the fold's manifest: {operations:?}"
            );
            assert_eq!(
                fixture.head_sources(),
                (positions + 1, anchors),
                "the read starts from the position, without reloading"
            );
        });
        folded
    });
    let folded = folded.expect("fold");
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
    let options = strong();
    let (refreshed, ()) = futures::join!(
        fixture.reader.stat_with_options("/small.txt", &options),
        async {
            gate.wait_until_blocked().await;
            fixture.drop_position().await;
            gate.release();
        }
    );
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
/// collection deleted drops the position. The check's HEAD finds no
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
    // Another process claims the compactor twice, and a collection one grace
    // window later deletes the manifests below the newest claim.
    for _ in 0..2 {
        loonfs_core::NamespaceEngine::writer(namespace(), writer_id("position-boundary-peer"))
            .claim_compactor(None)
            .await
            .expect("another process claims the compactor");
    }
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

/// Returns a metadata cache that keeps no manifest, so a writer position's
/// manifest is read from the store each time.
fn no_manifests() -> MetadataCache {
    MetadataCache::builder().max_manifest_bytes(0).build()
}

/// Creates a fixture whose runtime keeps no manifest, in which another
/// process claims the compactor twice and a collection one grace window later
/// deletes the manifest of the session's position. Returns the fixture and
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
    for _ in 0..2 {
        loonfs_core::NamespaceEngine::writer(namespace(), writer_id(&format!("{name}-peer")))
            .claim_compactor(None)
            .await
            .expect("another process claims the compactor");
    }
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

/// Returns the requests that load a writer position's manifest
/// `position`, which a collection deleted: a GET that finds nothing, a read
/// of the manifest boundary, which is at or above `position`, and the
/// discovery that finds the newest manifest, `newest`.
fn collected_position_load(
    namespace_id: &NamespaceId,
    position: ManifestNo,
    newest: ManifestNo,
) -> Vec<String> {
    let manifest =
        |number: ManifestNo| format!("get {namespace_id} MetadataManifest {:020}", number.0);
    vec![
        manifest(position),
        format!("get {namespace_id} Boundary"),
        format!("get {namespace_id} Boundary"),
        format!("list namespaces/{namespace_id}/manifests/"),
        manifest(newest),
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
/// collection deleted folds from the newest manifest. The position's
/// manifest isn't in the segment cache, so the fold reads the manifest from
/// the store, finds nothing, reads the manifest boundary, and loads the
/// newest manifest by discovery. No later request reads the deleted
/// manifest, and the position moves off the deleted manifest, so a strong
/// read after the fold doesn't start from that manifest.
#[tokio::test]
async fn a_fold_from_a_collected_position_starts_from_the_newest_manifest() {
    let name = "fold-collected";
    let (fixture, position) = collected_position(name).await;
    let newest = newest_manifest(&fixture).await;
    fixture.recording.reset();
    let folded = fixture
        .writer
        .maintenance(writer_id(name))
        .fold_wal(&fixture.namespace_id)
        .await
        .expect("the fold starts from the newest manifest");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let operations = fixture.recording.take();
    let load = collected_position_load(&fixture.namespace_id, position, newest);
    assert_eq!(
        loonfs_test_support::stores::request_lines(&operations[..load.len()]),
        load,
        "{operations:?}"
    );
    assert!(
        !uses_manifest(&operations[load.len()..], &fixture.namespace_id, position),
        "{operations:?}"
    );
    fixture
        .reader
        .stat_with_options("/small.txt", &strong())
        .await
        .expect("stat");
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
        "{operations:?}"
    );
}

/// Checks that a retention advance from a position whose manifest a
/// collection deleted reads the manifest from the store, finds nothing,
/// reads the manifest boundary, and plans from the newest manifest, and that
/// the advance drops the position, so a strong read after it doesn't start
/// from the deleted manifest.
#[tokio::test]
async fn a_retention_advance_from_a_collected_position_drops_the_position() {
    let name = "retention-collected";
    let (fixture, position) = collected_position(name).await;
    let newest = newest_manifest(&fixture).await;
    fixture.recording.reset();
    fixture
        .writer
        .maintenance(writer_id(name))
        .advance_retention_floor(&fixture.namespace_id)
        .await
        .expect("the advance plans from the newest manifest");
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.recording.take()),
        collected_position_load(&fixture.namespace_id, position, newest)
    );
    fixture
        .reader
        .stat_with_options("/small.txt", &strong())
        .await
        .expect("stat");
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
        "{operations:?}"
    );
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
        .writer
        .maintenance(writer_id(name))
        .fold_wal(&fixture.namespace_id)
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

/// Returns a fixture whose writer session keeps a position on a manifest
/// that a collection deleted, with the segments that only that manifest
/// lists, and whose runtime caches no segment's rows, so every read of a
/// segment goes to the store. With `cached`, the
/// runtime's segment cache still has the deleted manifest, which the
/// writer's own fold put there. The fixture also returns the bytes of the
/// deleted manifest.
///
/// The writer folds, so its position is on the fold's manifest. A peer then
/// folds the writer's next commits eight times and compacts while a unit is
/// due, which rewrites the position's runs. A collection one grace window
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
        .writer
        .maintenance(writer_id(name))
        .fold_wal(&namespace_id)
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
    let peer_id = format!("{name}-peer");
    let peer = peer(&fixture, &peer_id).await;
    let maintenance = peer.maintenance(writer_id(&peer_id));
    for index in 0..8 {
        fixture.put(&format!("/{index}.txt")).await.expect("put");
        maintenance.fold_wal(&namespace_id).await.expect("fold");
    }
    maintenance
        .maintain_metadata_while_due(&namespace_id, &loonfs::MaintenanceCancellation::default())
        .await
        .expect("compact");
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
/// collection deleted plans from the newest manifest, whether or not the
/// runtime's segment cache still has the deleted manifest. Every commit is
/// younger than the grace window, so the floor stays at 0.
///
/// Without the manifest in the cache, the advance reads the manifest from
/// the store, finds nothing, and reads the manifest boundary, which is at or
/// above the manifest, so the advance drops the position and plans from the
/// newest manifest. With the manifest in the cache, the plan reads a segment
/// that the collection deleted, the advance finds a newer current manifest,
/// drops the position and the cached head, and plans from the newest
/// manifest.
#[tokio::test]
async fn a_retention_advance_from_a_position_on_a_collected_manifest_succeeds() {
    for cached in [false, true] {
        let name = format!("collected-advance-{}", if cached { "hit" } else { "miss" });
        let (fixture, position, _) =
            position_on_a_collected_manifest(&name, cached, History::Files).await;
        fixture.recording.reset();
        let advanced = fixture
            .writer
            .maintenance(writer_id(&name))
            .advance_retention_floor(&fixture.namespace_id)
            .await;
        assert_eq!(
            advanced
                .unwrap_or_else(|error| panic!("cached={cached}: {error:?}"))
                .retention_floor_seq,
            loonfs::CommitSeq(0),
            "cached={cached}"
        );
        let namespace = fixture.namespace_id.to_string();
        let line = |request: &str| request.replace("{ns}", &namespace);
        let discovery = |newest_get: bool| {
            let mut lines = vec![
                line("get {ns} Boundary"),
                line("list namespaces/{ns}/manifests/"),
            ];
            if newest_get {
                lines.push(line("get {ns} MetadataManifest 00000000000000000018"));
            }
            lines.push(line("get {ns} Boundary"));
            lines
        };
        let segments = vec![line("get {ns} MetadataSegment"); 4];
        let expected = if cached {
            [
                // The plan's read of a segment that the collection deleted.
                vec![line("get {ns} MetadataSegment")],
                // The check that a newer manifest replaced the planned one.
                discovery(true),
                // The second plan, from the newest manifest.
                discovery(false),
                segments,
            ]
            .concat()
        } else {
            [
                vec![
                    line(&format!("get {{ns}} MetadataManifest {:020}", position.0)),
                    line("get {ns} Boundary"),
                ],
                discovery(true),
                segments,
            ]
            .concat()
        };
        assert_eq!(
            loonfs_test_support::stores::request_lines(&fixture.recording.take()),
            expected,
            "cached={cached}"
        );
    }
}

/// Checks that a fold from a writer position whose manifest a collection
/// deleted succeeds when the runtime's segment cache still has that manifest
/// and the position's tail appends to a file, and that the session reads,
/// commits and folds after it.
///
/// The fold writes the appended bytes after the file's first bytes, so it
/// reads the file's layout row, and the only segment that has that row is
/// one that the collection deleted after a compaction rewrote the layout
/// family. The fold finds that a newer manifest replaced the position's
/// manifest, finds the newest manifest, loads the tail above it, which has
/// the session's commit after the collection, and publishes the next
/// manifest. The session's position moves onto that manifest, so an
/// eventual read after the fold sends no manifest check and reads the new
/// manifest's segments.
#[tokio::test]
async fn a_fold_of_an_append_from_a_position_on_a_collected_manifest_succeeds() {
    let name = "collected-append-fold";
    let (fixture, _, _) = position_on_a_collected_manifest(name, true, History::AppendInTail).await;
    let maintenance = fixture.writer.maintenance(writer_id(name));
    fixture
        .put("/before.txt")
        .await
        .expect("a commit before the fold");
    fixture.recording.reset();
    let folded = maintenance
        .fold_wal(&fixture.namespace_id)
        .await
        .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let namespace = fixture.namespace_id.to_string();
    let line = |request: &str| request.replace("{ns}", &namespace);
    let expected = [
        // The lookup of the file's layout row in a segment that the
        // collection deleted.
        vec![line("get {ns} MetadataSegment")],
        // The check that a newer manifest replaced the position's manifest.
        [
            "get {ns} Boundary",
            "list namespaces/{ns}/manifests/",
            "get {ns} MetadataManifest 00000000000000000018",
            "get {ns} Boundary",
        ]
        .map(line)
        .to_vec(),
        // The next attempt finds the newest manifest above the position's.
        [
            "head {ns} MetadataManifest 00000000000000000005",
            "get {ns} Boundary",
            "get {ns} Boundary",
            "list namespaces/{ns}/manifests/",
            "get {ns} Boundary",
        ]
        .map(line)
        .to_vec(),
        // The rebuild loads the tail above the newest manifest.
        [
            "get {ns} Boundary",
            "list namespaces/{ns}/manifests/",
            "get {ns} Boundary",
            "get {ns} WalObject 00000000000000000012",
            "get {ns} WalObject 00000000000000000013",
            "get {ns} WalObject 00000000000000000014",
            "head {ns} MetadataManifest 00000000000000000019",
            "get {ns} Boundary",
        ]
        .map(line)
        .to_vec(),
        // The fold writes the commit's content, its segments and the
        // manifest, and settles the put against the boundary.
        vec![line("assemble pool ContentBlob")],
        vec![line("put {ns} MetadataSegment"); 7],
        [
            "put {ns} MetadataManifest 00000000000000000019",
            "get {ns} Boundary",
        ]
        .map(line)
        .to_vec(),
    ]
    .concat();
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.recording.take()),
        expected
    );
    fixture
        .reader
        .stat("/before.txt")
        .await
        .expect("an eventual read after the fold");
    let operations = fixture.recording.take();
    assert!(
        loonfs_test_support::stores::request_lines(&operations)
            .iter()
            .all(|request| *request == line("get {ns} MetadataSegment")),
        "the read starts from the position on the fold's manifest, and reads \
         only the segments that the runtime doesn't cache: {operations:?}"
    );
    fixture
        .put("/after.txt")
        .await
        .expect("a commit after the fold");
    assert_eq!(
        maintenance
            .fold_wal(&fixture.namespace_id)
            .await
            .expect("a fold after the commit")
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
    let maintenance = fixture.writer.maintenance(writer_id(name));
    maintenance
        .fold_wal(&fixture.namespace_id)
        .await
        .expect("fold the file");
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
    let error = maintenance
        .fold_wal(&fixture.namespace_id)
        .await
        .expect_err("a missing segment of the current manifest");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error:?}");
    assert!(format!("{error:?}").contains("MissingSegment"), "{error:?}");
}

/// Checks that the sweep's fold, which a writer runtime's host runs once
/// the session's tail is older than the idle fold age, and a user snapshot,
/// which folds first, succeed from the state of
/// [`a_fold_of_an_append_from_a_position_on_a_collected_manifest_succeeds`].
/// The sweep's fold finds that a newer manifest already folds the tail and
/// returns `AlreadyCurrent`.
#[tokio::test]
async fn a_sweep_and_a_snapshot_from_a_position_on_a_collected_manifest_succeed() {
    for snapshot in [false, true] {
        let name = if snapshot {
            "collected-append-snapshot"
        } else {
            "collected-append-sweep"
        };
        let (fixture, _, _) =
            position_on_a_collected_manifest(name, true, History::AppendInTail).await;
        if snapshot {
            fixture
                .namespace
                .create_snapshot()
                .await
                .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
        } else {
            fixture.clock.advance_ms(
                loonfs::RuntimeConfig::default()
                    .idle_fold_after_ms
                    .expect("idle fold age")
                    .get(),
            );
            let swept = fixture
                .writer
                .maintenance(writer_id(name))
                .maintain_metadata(&fixture.namespace_id)
                .await
                .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
            assert_eq!(
                swept.wal_fold.map(|fold| fold.outcome),
                Some(FoldWalOutcome::AlreadyCurrent)
            );
        }
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
                .metadata_with_options(&loonfs::LiveReadOptions::default())
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
    fixture
        .writer
        .maintenance(writer_id(name))
        .fold_wal(&fork)
        .await
        .expect("fold the fork");
    fixture.writer.drain().await.expect("drain");
    let position = loonfs_core::test_support::namespace_store(store.clone(), &fork)
        .load_current_manifest()
        .await
        .expect("the fork position's manifest");
    let peer_id = format!("{name}-peer");
    let peer = peer(&fixture, &peer_id).await;
    let maintenance = peer.maintenance(writer_id(&peer_id));
    for index in 0..8 {
        fork_namespace
            .put_file(&format!("/{index}.txt"), b"file", &actor)
            .await
            .expect("put");
        maintenance.fold_wal(&fork).await.expect("fold");
    }
    maintenance
        .maintain_metadata_while_due(&fork, &loonfs::MaintenanceCancellation::default())
        .await
        .expect("compact");
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
/// segments that a collection deleted plans from the newest manifest when
/// the runtime's segment cache doesn't have the manifest.
///
/// The fixture's collection deletes the manifest, and the test puts the
/// manifest back by hand. The advance reads the manifest from the store and
/// plans from it as it would from the segment cache, the plan reads a
/// segment that the collection deleted, and the advance finds a newer
/// current manifest, drops the position and the cached head, and plans from
/// the newest manifest.
#[tokio::test]
async fn a_retention_advance_from_a_stored_position_manifest_below_the_boundary_succeeds() {
    use loonfs_objectstore::ObjectStore;
    let name = "stored-below-boundary";
    let (fixture, position, manifest_bytes) =
        position_on_a_collected_manifest(name, false, History::Files).await;
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
    let advanced = fixture
        .writer
        .maintenance(writer_id(name))
        .advance_retention_floor(&fixture.namespace_id)
        .await
        .expect("the advance plans from the newest manifest");
    assert_eq!(advanced.retention_floor_seq, loonfs::CommitSeq(0));
    let namespace = fixture.namespace_id.to_string();
    let line = |request: &str| request.replace("{ns}", &namespace);
    // The runtime caches no manifest, so each discovery reads the newest.
    let discovery = vec![
        line("get {ns} Boundary"),
        line("list namespaces/{ns}/manifests/"),
        line("get {ns} MetadataManifest 00000000000000000018"),
        line("get {ns} Boundary"),
    ];
    let expected = [
        // The load of the position's manifest, and the plan's read of a
        // segment that the collection deleted.
        vec![
            line(&format!("get {{ns}} MetadataManifest {:020}", position.0)),
            line("get {ns} MetadataSegment"),
        ],
        // The check that a newer manifest replaced the planned one.
        discovery.clone(),
        // The second plan, from the newest manifest.
        discovery,
        vec![line("get {ns} MetadataSegment"); 4],
    ]
    .concat();
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.recording.take()),
        expected
    );
}

/// Checks that a strong read whose cached head is on a manifest that a
/// collection deleted reads again from the newest manifest, and checks the
/// read's requests.
///
/// The writer's session closes, so its position, on the deleted manifest,
/// becomes the cached head. The strong read's WAL probe finds nothing new,
/// and the manifest check isn't due. The read then finds a segment of the
/// deleted manifest missing, finds the newer current manifest, and drops the
/// runtime's view of the namespace. The second run loads the head by
/// discovery and reads from the newest manifest.
#[tokio::test]
async fn a_strong_read_from_a_collected_manifest_reads_the_newest_manifest() {
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
        .read_file_with_options(
            "/small.txt",
            &ReadFileOptions {
                at: ReadAt::Live(Some(Consistency::Strong)),
            },
        )
        .await
        .expect("the read runs again from the newest manifest");
    assert_eq!(file.bytes, [b's'; 1024]);
    let namespace = fixture.namespace_id.to_string();
    let line = |request: &str| request.replace("{ns}", &namespace);
    let expected = [
        // The first run: the WAL probe of the cached head, and the read of a
        // segment that the collection deleted.
        line("get {ns} WalObject 00000000000000000011"),
        line("get {ns} MetadataSegment"),
        // The check that a newer manifest replaced the read's manifest.
        line("get {ns} Boundary"),
        line("list namespaces/{ns}/manifests/"),
        line("get {ns} MetadataManifest 00000000000000000018"),
        line("get {ns} Boundary"),
        // The second run's load of the head by discovery, with the WAL read
        // above the newest manifest and the check of its successor.
        line("get {ns} Boundary"),
        line("list namespaces/{ns}/manifests/"),
        line("get {ns} Boundary"),
        line("get {ns} WalObject 00000000000000000011"),
        line("head {ns} MetadataManifest 00000000000000000019"),
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

/// Checks that a commit whose inline content is too large for a WAL object
/// commits from a writer position whose manifest a collection deleted, when
/// neither the segment cache nor the store has the manifest. The receipt
/// check before the content is staged finds the manifest missing at or below
/// the manifest boundary, and runs again from the newest manifest.
#[tokio::test]
async fn a_commit_that_stages_content_from_a_collected_position_succeeds() {
    use loonfs::publish::{CommitCandidate, CommitIntent, FilesystemOperation, InlineContent};
    let (fixture, _, _) =
        position_on_a_collected_manifest("collected-staging", false, History::Files).await;
    let value = InlineContent::new(
        loonfs_types::ContentId::generate(),
        bytes::Bytes::from(vec![
            b'l';
            loonfs_types::format::wal::MAX_WAL_INLINE_CONTENT_BYTES
                + 1
        ]),
        ChecksumAlgorithm::Crc64nvme,
    );
    let intent = CommitIntent::single(
        loonfs_types::CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: loonfs_types::AbsolutePath::parse("/staged.txt").expect("path"),
            content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
            behavior: loonfs_types::DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    fixture
        .namespace
        .commit_candidate(CommitCandidate::with_inline_content(
            intent,
            Vec::new(),
            vec![value],
        ))
        .await
        .unwrap_or_else(|error| panic!("{:?} {error:?}", error.code()));
}

/// Checks that the reads of a subject's administrator rights succeed
/// from a cached head whose manifest a collection deleted, when neither the
/// segment cache nor the store has the manifest: a deletion, a lookup of a
/// snapshot, a listing of a snapshot's files, and a stat at a snapshot.
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
                .list_snapshot_files(&snapshot_id)
                .page()
                .await
                .map(drop),
            _ => reader
                .stat_with_options(
                    "/small.txt",
                    &StatOptions {
                        at: ReadAt::Snapshot(snapshot_id),
                        ..StatOptions::default()
                    },
                )
                .await
                .map(drop),
        };
        result.unwrap_or_else(|error| panic!("{case}: {:?} {error:?}", error.code()));
    }
}
