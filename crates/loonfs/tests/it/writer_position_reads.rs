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
    let writer = LoonFs::builder_with_store(store)
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

    /// Forks the namespace, which drops the session's position and the
    /// cached head, so the next read loads the namespace from the store.
    pub(crate) async fn drop_position(&self) {
        self.writer
            .fork_namespace(
                &self.namespace_id,
                &namespace_id(&format!("{}-dropped", self.namespace_id)),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("fork");
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
/// while a publish has the engine locked, loads the fold's manifest, finds
/// nothing after it, waits for the publish to release the engine, and then
/// moves the position onto the fold's manifest without reloading, so reads
/// start from the fold's manifest.
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
                checked.len() >= 3
                    && *checked[0] == manifest(fold_manifest)
                    && is_manifest_get(checked[1], &fixture.namespace_id, fold_manifest)
                    && is_manifest_get(checked[2], &fixture.namespace_id, after_fold)
                    && checked[3..].iter().all(|operation| {
                        is_manifest_get(operation, &fixture.namespace_id, fold_manifest)
                    }),
                "one HEAD finds the fold's manifest, the check loads it and finds nothing \
                 after it, and the read starts from the fold's manifest: {operations:?}"
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
///
/// A fork of the current head folds the source's WAL tail without reporting
/// the fold, so the runtime drops the source's position and the cached head
/// that the position becomes.
#[tokio::test]
async fn a_change_of_unknown_effect_drops_the_view_and_the_next_read_loads_it() {
    let fixture = fixture("unknown-change", Parked::WalPuts).await;
    let commit = fixture.put("/next.txt").await.expect("commit");
    fixture
        .writer
        .fork_namespace(
            &fixture.namespace_id,
            &namespace_id("unknown-change-fork"),
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
        matches!(
            operations.first(),
            Some(RecordedOperation::GetWithMetadata { key, .. })
                if *key == boundary(&fixture.namespace_id)
        ),
        "the read loads the namespace from its manifest boundary: {operations:?}"
    );
    assert_eq!(fixture.head_sources(), (positions, anchors + 1));
}

/// Checks that a refresh that started before a change of unknown effect,
/// and finishes after the runtime dropped its view for that change, installs
/// nothing, so the next read loads the namespace by discovery.
///
/// A fork drops the position, and a read loads the namespace into the read
/// cache. A strong read's refresh of that view then
/// waits at its WAL probe while a fork, whose fold of the source's tail
/// isn't reported, drops the view. The refresh's head is from before the
/// fork, so an eventual read after it would otherwise start from that head
/// without a request.
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
            fixture
                .writer
                .fork_namespace(
                    &fixture.namespace_id,
                    &namespace_id("spanning-refresh-fork"),
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("fork");
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
            .claim_compactor()
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
            .claim_compactor()
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

/// Checks that the session's fold from a position whose manifest a
/// collection deleted folds from the newest manifest. The position's
/// manifest isn't in the segment cache, so the fold checks the manifest
/// against the manifest boundary and doesn't read it, and the position moves
/// off the deleted manifest, so a strong read after the fold doesn't start
/// from it.
#[tokio::test]
async fn a_fold_from_a_collected_position_starts_from_the_newest_manifest() {
    let name = "fold-collected";
    let (fixture, position) = collected_position(name).await;
    fixture.recording.reset();
    let folded = fixture
        .writer
        .maintenance(writer_id(name))
        .fold_wal(&fixture.namespace_id)
        .await
        .expect("the fold starts from the newest manifest");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
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
/// collection deleted plans from the newest manifest, and that the advance
/// drops the position, so a strong read after it doesn't start from the
/// deleted manifest.
#[tokio::test]
async fn a_retention_advance_from_a_collected_position_drops_the_position() {
    let name = "retention-collected";
    let (fixture, position) = collected_position(name).await;
    fixture.recording.reset();
    fixture
        .writer
        .maintenance(writer_id(name))
        .advance_retention_floor(&fixture.namespace_id)
        .await
        .expect("the advance plans from the newest manifest");
    let operations = fixture.recording.take();
    assert!(
        !uses_manifest(&operations, &fixture.namespace_id, position),
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
