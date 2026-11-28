//! Manifest publications that build on a live session's writer position,
//! which put the position's successor without a request first and read
//! neither the manifest boundary nor a WAL object before the put. A
//! retention advance checks the position first, and its fold then puts the
//! successor of the checked position. The session's manifest publications
//! take their turns in its publication lane, each at its own manifest
//! number, while its commits continue.

use super::*;
use loonfs_core::ExtensionPublication;
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::format::wal::WalObjectBody;
use loonfs_types::{ExtensionName, ManifestNo};
use std::num::NonZeroU64;

type Recording = Arc<RecordingStore<LocalFsStore>>;

struct Fixture {
    _directory: tempfile::TempDir,
    store: Recording,
    /// Blocks a create-if-absent put of a manifest of the namespace once the
    /// test arms it.
    manifest_gate: Arc<BlockingStore<Recording>>,
    /// Blocks a create-if-absent put of a snapshot record of the namespace
    /// once the test arms it.
    snapshot_gate: Arc<BlockingStore<Arc<BlockingStore<Recording>>>>,
    writer: crate::LoonFs<crate::Writable>,
    namespace_id: NamespaceId,
    namespace: crate::Namespace<crate::Writable>,
}

impl Fixture {
    /// Creates a writer built by `configure`, with its namespace created and
    /// opened, and the session's first commit published.
    async fn new(
        configure: impl FnOnce(
            crate::LoonFsBuilder<crate::Writable>,
        ) -> crate::LoonFsBuilder<crate::Writable>,
    ) -> Self {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("basis").expect("namespace id");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let manifest_gate = Arc::new(BlockingStore::new(
            Arc::clone(&store),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::PutCreateIfAbsent,
        ));
        let snapshot_gate = Arc::new(BlockingStore::new(
            Arc::clone(&manifest_gate),
            KeyPredicate::prefix(loonfs_objectstore::keys::snapshot_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
        ));
        let builder = crate::LoonFs::builder_with_store(
            snapshot_gate.clone() as SharedStore,
            crate::Context::Service,
        )
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .monotonic_timer(Arc::new(ManualClock::new(0)));
        let writer = configure(builder).build().await.expect("build writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "first", "first",
            )))
            .await
            .expect("the session's first commit");
        writer.drain().await.expect("drain");
        Self {
            _directory: directory,
            store,
            manifest_gate,
            snapshot_gate,
            writer,
            namespace_id,
            namespace,
        }
    }

    /// Returns the numbers of the manifest puts in the request log, in the
    /// order of the log, and clears the log.
    fn manifest_puts(&self) -> Vec<ManifestNo> {
        let manifests = metadata_manifest_prefix(&self.namespace_id);
        self.store
            .take()
            .iter()
            .filter(|operation| {
                matches!(operation, RecordedOperation::Put { .. })
                    && operation.key().starts_with(&manifests)
            })
            .map(|operation| manifest_no_of(operation.key()).expect("a manifest key"))
            .collect()
    }

    /// Returns the snapshot record puts in the request log.
    fn snapshot_record_puts(&self) -> usize {
        let snapshots = loonfs_objectstore::keys::snapshot_prefix(&self.namespace_id);
        self.store
            .snapshot()
            .iter()
            .filter(|operation| {
                matches!(operation, RecordedOperation::Put { .. })
                    && operation.key().starts_with(&snapshots)
            })
            .count()
    }

    /// Clears the request log.
    fn reset_requests(&self) {
        self.store.reset();
    }

    fn manifest(&self, manifest_no: ManifestNo) -> String {
        loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &manifest_no)
    }

    /// Returns the logged manifest requests, and checks that the log has no
    /// read of a WAL object and no request for the manifest boundary before
    /// the first manifest put.
    fn manifest_requests(&self) -> Vec<(&'static str, String)> {
        let operations = self.store.take();
        let wal = wal_prefix(&self.namespace_id);
        let boundary = loonfs_objectstore::keys::boundary(&self.namespace_id);
        let manifests = metadata_manifest_prefix(&self.namespace_id);
        let first_put = operations
            .iter()
            .position(|operation| {
                matches!(operation, RecordedOperation::Put { .. })
                    && operation.key().starts_with(&manifests)
            })
            .unwrap_or(operations.len());
        assert!(
            !operations[..first_put]
                .iter()
                .any(|operation| operation.key() == boundary),
            "{operations:?}"
        );
        assert!(
            !operations
                .iter()
                .any(|operation| operation.key().starts_with(&wal)
                    && !matches!(operation, RecordedOperation::Put { .. })),
            "{operations:?}"
        );
        operations
            .iter()
            .filter(|operation| operation.key().starts_with(&manifests))
            .map(|operation| {
                let kind = match operation {
                    RecordedOperation::Head { .. } => "head",
                    RecordedOperation::Put { .. } => "put",
                    _ => "get",
                };
                (kind, operation.key().to_owned())
            })
            .collect()
    }

    /// Returns whether the namespace's WAL has a floor object.
    async fn has_floor_object(&self) -> bool {
        for key in self
            .store
            .list_prefix(&wal_prefix(&self.namespace_id))
            .await
            .expect("list the WAL")
        {
            let bytes = self
                .store
                .get(&key, None)
                .await
                .expect("read a WAL object")
                .expect("the WAL object exists");
            let payload = decode_wal_object_envelope_zstd(&bytes)
                .expect("decode a WAL object")
                .into_payload();
            if matches!(payload.body, WalObjectBody::Floor { .. }) {
                return true;
            }
        }
        false
    }

    async fn current_manifest_no(&self) -> ManifestNo {
        current_manifest_no(self.store.as_ref(), &self.namespace_id).await
    }

    async fn shut_down(self) {
        self.writer.shutdown().await.expect("shut down writer");
    }
}

/// Checks that an explicit fold in a runtime with the namespace's live
/// session folds the session's position, and puts the position's successor
/// without a request first.
#[tokio::test]
async fn an_explicit_fold_from_a_live_session_puts_on_its_basis_with_no_request_first() {
    let fixture = Fixture::new(|builder| builder).await;
    fixture.writer.drain().await.expect("drain");
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.reset_requests();
    let folded = bounded(
        Duration::from_secs(10),
        fixture
            .writer
            .open_namespace(&fixture.namespace_id)
            .expect("open namespace")
            .fold_wal(),
    )
    .await
    .expect("the fold returns")
    .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    assert_eq!(folded.manifest_no, next);
    assert_eq!(
        fixture.manifest_requests(),
        vec![("put", fixture.manifest(next))]
    );
    fixture.shut_down().await;
}

/// Checks that a retention advance in a runtime with the namespace's live
/// session checks the session's position as a strong read does, creates its
/// floor object at the session's tip, and then folds the floor object on the
/// session's position. The check is a HEAD of the manifest after the
/// position's manifest and a read of the manifest boundary, and the fold's
/// put is the advance's one manifest put.
#[tokio::test]
async fn a_retention_advance_from_a_live_session_checks_the_position_and_folds_on_its_basis() {
    let clock = Arc::new(ManualClock::at_system_time());
    let fixture = Fixture::new({
        let clock = Arc::clone(&clock);
        move |builder| builder.wall_clock(clock)
    })
    .await;
    let maintenance = fixture.writer.clone();
    maintenance
        .open_namespace(&fixture.namespace_id)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold the first commit");
    fixture.writer.drain().await.expect("drain");
    // Every commit is a grace window old, so the advance can pass them.
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.reset_requests();
    let advanced = bounded(
        Duration::from_secs(10),
        maintenance
            .open_namespace(&fixture.namespace_id)
            .expect("open namespace")
            .advance_retention_floor(),
    )
    .await
    .expect("the advance returns")
    .expect("advance the floor");
    assert!(advanced.retention_floor_seq > CommitSeq(0));
    let manifest = format!("basis MetadataManifest {:020}", next.0);
    let expected = [
        // The check of the position.
        vec![format!("head {manifest}"), "get basis Boundary".to_owned()],
        // The check that the planned manifest's segments exist.
        vec!["head basis MetadataSegment".to_owned(); 5],
        // The floor object, and the fold that records its floor.
        vec![
            "put basis WalObject 00000000000000000003".to_owned(),
            format!("put {manifest}"),
            "get basis Boundary".to_owned(),
        ],
    ]
    .concat();
    assert_eq!(
        loonfs_test_support::stores::request_lines(&fixture.store.take()),
        expected
    );
    fixture.shut_down().await;
}

/// Checks that the session's fold at the write stop puts the position's
/// successor without a request first, and that the commit waiting for the
/// fold then commits.
#[tokio::test]
async fn a_fold_at_the_write_stop_puts_on_its_basis_with_no_request_first() {
    let fixture = Fixture::new(|builder| builder).await;
    let publisher = fixture.namespace.session().publisher.clone();
    let tail_objects = || async {
        publisher
            .engine
            .lock()
            .await
            .wal_tail_objects()
            .unwrap_or(0)
    };
    // No fold starts while the test has every fold permit, so the session
    // commits until its tail reaches the write stop.
    let permits = hold_every_fold_permit(&fixture.writer).await;
    for fill in 0.. {
        if tail_objects().await >= MAX_UNFOLDED_WAL_OBJECTS {
            break;
        }
        let name = format!("fill-{fill}");
        fixture
            .namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(&name, &name)))
            .await
            .expect("fill the tail to the write stop");
    }
    let namespace = fixture.namespace.clone();
    let commit = tokio::spawn(async move {
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "at-the-stop",
                "at-the-stop",
            )))
            .await
    });
    bounded(Duration::from_secs(10), async {
        while queued_candidates(&publisher_state(&publisher)) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the commit waits at the write stop");
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.reset_requests();
    drop(permits);
    bounded(Duration::from_secs(10), commit)
        .await
        .expect("the commit settles")
        .expect("join the commit")
        .expect("the commit is committed after the fold");
    let requests = fixture.manifest_requests();
    assert_eq!(
        requests[..1],
        [("put", fixture.manifest(next))],
        "{requests:?}"
    );
    fixture.shut_down().await;
}

/// Takes every compaction permit, and then writes files until a write's fold
/// makes the session's compaction wait for a permit, and returns the permits.
async fn write_until_compaction_waits<'a>(
    fixture: &'a Fixture,
    files: &mut usize,
) -> Vec<BudgetPermit<'a>> {
    let budget = fixture.writer.execution_budget();
    let publisher = fixture.namespace.session().publisher.clone();
    let mut permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS.get() {
        permits.push(budget.compaction_permit().await);
    }
    for _ in 0..32 {
        *files += 1;
        fixture
            .namespace
            .put_file(
                &format!("/file-{files}"),
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put a file");
        fixture
            .namespace
            .wait_for_fold()
            .await
            .expect("the write's fold settles");
        // The fold starts the session's compaction before its task exits,
        // and the compaction either finds nothing due and exits, or waits
        // for a permit.
        let waits = bounded(Duration::from_secs(10), async {
            tokio::select! {
                finished = publisher.wait_for_compaction() => {
                    finished.expect("the compaction finds nothing due");
                    false
                }
                () = async {
                    while budget.stats().compactions_waiting == 0 {
                        tokio::task::yield_now().await;
                    }
                } => true,
            }
        })
        .await
        .expect("the session's compaction settles or waits");
        if waits {
            return permits;
        }
    }
    panic!("each write's fold adds a run, so compaction should become due");
}

/// Checks that a session's compaction unit plans from the session's
/// position, and puts the position's successor without a request first.
#[tokio::test]
async fn a_session_compaction_unit_puts_on_its_basis_with_no_request_first() {
    // The fold threshold is one WAL object, so each write is folded.
    let fixture = Fixture::new(|builder| {
        builder
            .session_fold_at_wal_objects(NonZeroU64::MIN)
            .config(crate::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..crate::RuntimeConfig::default()
            })
    })
    .await;
    let mut files = 0;
    let permits = write_until_compaction_waits(&fixture, &mut files).await;
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.reset_requests();
    drop(permits);
    bounded(Duration::from_secs(10), fixture.writer.drain())
        .await
        .expect("the compaction settles")
        .expect("drain");
    let requests = fixture.manifest_requests();
    assert_eq!(
        requests[..1],
        [("put", fixture.manifest(next))],
        "{requests:?}"
    );
    fixture.shut_down().await;
}

/// Checks what a publication returns when its position is behind on both of
/// its runs: `writer_fenced` when another writer claimed the namespace after
/// the session loaded its position again, which the server sends once more
/// in a new session, and `head_moved` when the session is still the writer,
/// so both manifests that it lost to were its own, such as puts whose
/// outcome was unknown.
#[tokio::test]
async fn a_publication_behind_on_both_runs_returns_what_the_writers_load_finds() {
    for claimed in [true, false] {
        let fixture = Fixture::new(|builder| builder).await;
        let publisher = fixture.namespace.session().publisher.clone();
        let rival = crate::LoonFs::builder_with_store(
            fixture.store.clone() as SharedStore,
            crate::Context::Service,
        )
        .writer_id(loonfs_test_support::ids::writer_id("rival"))
        .build()
        .await
        .expect("the rival's runtime");
        let runs = std::sync::atomic::AtomicUsize::new(0);
        let (rival_ref, runs_ref, namespace_id) = (&rival, &runs, &fixture.namespace_id);
        let published: crate::Result<()> = publisher
            .publish_from(
                || publisher.writer_position(),
                move |_position| {
                    let run = runs_ref.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async move {
                        if claimed && run == 1 {
                            rival_ref
                                .open_namespace(namespace_id)
                                .expect("open on the rival")
                                .create_directory("/rival", &loonfs_test_support::test_actor())
                                .await
                                .expect("the rival claims with a commit");
                        }
                        Err(CoreError::HeadMoved("behind".to_owned()))
                    }
                },
                |_| false,
            )
            .await;
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 2);
        let error = published.expect_err("both runs are behind");
        let expected = if claimed {
            crate::ErrorCode::WriterFenced
        } else {
            crate::ErrorCode::StoreContention
        };
        assert_eq!(error.code(), expected, "claimed={claimed}: {error:?}");
        assert_matches!(
            (claimed, &error),
            (true, Error::Core(CoreError::WriterFenced(_)))
                | (false, Error::Core(CoreError::HeadMoved(_)))
        );
        rival.shutdown().await.expect("shutdown the rival");
        fixture.writer.shutdown().await.expect("shutdown");
    }
}

/// Returns a retention window that differs from every earlier one in a test,
/// so each change publishes a manifest.
fn retention_window(extra_ms: u64) -> crate::RetentionWindow {
    crate::RetentionWindow::new(2 * crate::GC_GRACE_WINDOW_MS + extra_ms)
        .expect("a window longer than the grace window")
}

/// Returns the manifest numbers from `first` on, `count` of them.
fn numbers_from(first: ManifestNo, count: usize) -> Vec<ManifestNo> {
    std::iter::successors(Some(first), |manifest_no| manifest_no.successor().ok())
        .take(count)
        .collect()
}

/// Checks that a fold and a compaction unit of one session never put one
/// manifest number (the problem measured in `small-publication-rate-20261126`):
/// both are items of the session's publication lane, so the fold that a
/// commit starts while the unit's put is in flight waits for the unit, and
/// then puts the next number. The commit is acknowledged meanwhile.
#[tokio::test]
async fn a_fold_and_a_compaction_never_put_one_manifest_number() {
    // The fold threshold is one WAL object, so each write is folded.
    let fixture =
        Fixture::new(|builder| builder.session_fold_at_wal_objects(NonZeroU64::MIN)).await;
    let mut files = 0;
    let permits = write_until_compaction_waits(&fixture, &mut files).await;
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    fixture.reset_requests();
    fixture.manifest_gate.block_next();
    drop(permits);
    bounded(
        Duration::from_secs(10),
        fixture.manifest_gate.wait_until_blocked(),
    )
    .await
    .expect("the compaction unit reaches its manifest put");
    bounded(
        Duration::from_secs(10),
        fixture
            .namespace
            .put_file("/during", b"body", &loonfs_test_support::test_actor()),
    )
    .await
    .expect("the commit is acknowledged while the unit's put is in flight")
    .expect("put the file whose commit starts a fold");
    assert_matches!(
        bounded(
            Duration::from_millis(200),
            fixture.namespace.wait_for_fold()
        )
        .await,
        Err(_),
        "the fold waits for the compaction unit's put"
    );
    fixture.manifest_gate.release();
    bounded(Duration::from_secs(10), fixture.writer.drain())
        .await
        .expect("the fold and the unit settle")
        .expect("drain");
    let puts = fixture.manifest_puts();
    assert!(puts.len() >= 2, "the unit and the fold publish: {puts:?}");
    assert_eq!(
        puts,
        numbers_from(next, puts.len()),
        "one manifest put per number"
    );
    fixture.shut_down().await;
}

/// Checks that the writer's snapshot isn't refused by the session's own
/// window change: the snapshot writes and verifies its record as a
/// publication of the session's lane, so a window change that starts while
/// the record's put is in flight waits for the verification, and the
/// snapshot writes one record.
#[tokio::test]
async fn the_writers_snapshot_is_not_refused_by_its_own_window_change() {
    let fixture = Fixture::new(|builder| builder).await;
    fixture.namespace.fold_wal().await.expect("fold the tail");
    fixture.writer.drain().await.expect("drain");
    fixture.reset_requests();
    fixture.snapshot_gate.block_next();
    let snapshotting = fixture.namespace.clone();
    let snapshot = tokio::spawn(async move { snapshotting.create_snapshot().await });
    bounded(
        Duration::from_secs(10),
        fixture.snapshot_gate.wait_until_blocked(),
    )
    .await
    .expect("the snapshot reaches its record put");
    {
        let mut change = std::pin::pin!(fixture
            .namespace
            .set_retention_window(Some(retention_window(0))));
        assert_matches!(
            bounded(Duration::from_millis(200), change.as_mut()).await,
            Err(_),
            "the window change waits for the snapshot's verification"
        );
        fixture.snapshot_gate.release();
        change.await.expect("change the window");
    }
    bounded(Duration::from_secs(10), snapshot)
        .await
        .expect("the snapshot settles")
        .expect("join the snapshot")
        .expect("the snapshot is verified at its first attempt");
    assert_eq!(fixture.snapshot_record_puts(), 1);
    fixture.shut_down().await;
}

/// Checks that a claim waits for an item of the session's publication lane
/// in flight (`writer_lane` `MC_TwoQueuesClaimWaitsWitness`): a new session
/// without its writer epoch writes a snapshot's record as an item of its
/// lane, and a commit that arrives while the record's put is in flight
/// claims only after the snapshot's verification, so the snapshot isn't
/// refused by the claim's manifest.
#[tokio::test]
async fn a_claim_waits_for_a_lane_item_in_flight() {
    let fixture = Fixture::new(|builder| builder).await;
    fixture.namespace.fold_wal().await.expect("fold the tail");
    fixture.writer.drain().await.expect("drain");
    let other = crate::LoonFs::builder_with_store(
        fixture.snapshot_gate.clone() as SharedStore,
        crate::Context::Service,
    )
    .writer_id(loonfs_test_support::ids::writer_id("writer-b"))
    .config(crate::RuntimeConfig {
        min_publish_interval_ms: 0,
        ..crate::RuntimeConfig::default()
    })
    .monotonic_timer(Arc::new(ManualClock::new(0)))
    .build()
    .await
    .expect("build the other writer");
    let namespace = other
        .open_namespace(&fixture.namespace_id)
        .expect("open a new session");
    fixture.reset_requests();
    fixture.snapshot_gate.block_next();
    let snapshotting = namespace.clone();
    let snapshot = tokio::spawn(async move { snapshotting.create_snapshot().await });
    bounded(
        Duration::from_secs(10),
        fixture.snapshot_gate.wait_until_blocked(),
    )
    .await
    .expect("the snapshot reaches its record put");
    let mut commit = std::pin::pin!(namespace.commit_candidate(CommitCandidate::new(
        create_directory_request("claimed", "claimed")
    )));
    assert_matches!(
        bounded(Duration::from_millis(200), commit.as_mut()).await,
        Err(_),
        "the commit's claim waits for the snapshot"
    );
    assert_eq!(
        fixture.manifest_puts(),
        [],
        "no claim manifest while the snapshot's record put is in flight"
    );
    fixture.snapshot_gate.release();
    commit.await.expect("the commit claims and commits");
    bounded(Duration::from_secs(10), snapshot)
        .await
        .expect("the snapshot settles")
        .expect("join the snapshot")
        .expect("the snapshot is verified at its first attempt");
    assert_eq!(fixture.snapshot_record_puts(), 1);
    other.shutdown().await.expect("shut down the other writer");
}

/// Checks that a window change and an extension's entry take their turns
/// with a fold in the session's publication lane: both wait while the
/// fold's put is in flight, and then each puts the next manifest number
/// once, on the manifest before it, without a collision or a rebuild.
#[tokio::test]
async fn a_window_change_and_an_extension_entry_take_their_turns_with_a_fold() {
    let fixture = Fixture::new(|builder| builder).await;
    let next = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    let name = ExtensionName::parse("grep").expect("extension name");
    let basis = fixture
        .namespace
        .load_extension_basis()
        .await
        .expect("the extension's basis");
    fixture.reset_requests();
    fixture.manifest_gate.block_next();
    let folding = fixture.namespace.clone();
    let fold = tokio::spawn(async move { folding.fold_wal().await });
    bounded(
        Duration::from_secs(10),
        fixture.manifest_gate.wait_until_blocked(),
    )
    .await
    .expect("the fold reaches its manifest put");
    {
        let mut change = std::pin::pin!(fixture
            .namespace
            .set_retention_window(Some(retention_window(0))));
        let mut entry = std::pin::pin!(fixture.namespace.publish_extension(
            &basis,
            &name,
            ExtensionEntry::new(1, Vec::new(), serde_json::Map::new()),
        ));
        assert_matches!(
            bounded(Duration::from_millis(200), async {
                futures::future::select(change.as_mut(), entry.as_mut()).await;
            })
            .await,
            Err(_),
            "the window change and the entry wait for the fold's put"
        );
        fixture.manifest_gate.release();
        let (changed, published) = tokio::join!(change, entry);
        changed.expect("change the window");
        assert_matches!(
            published.expect("publish the entry"),
            ExtensionPublication::Published { rebuilt: false, .. }
        );
    }
    let folded = bounded(Duration::from_secs(10), fold)
        .await
        .expect("the fold settles")
        .expect("join the fold")
        .expect("fold");
    assert_eq!(folded.manifest_no, next);
    assert_eq!(
        fixture.manifest_requests(),
        numbers_from(next, 3)
            .into_iter()
            .map(|manifest_no| ("put", fixture.manifest(manifest_no)))
            .collect::<Vec<_>>()
    );
    fixture.shut_down().await;
}

/// Checks that a commit is acknowledged while a publication of the session's
/// lane has its put in flight, because the commit queue and the lane are
/// separate queues. This pins behavior that the lane keeps: a commit doesn't
/// wait for a manifest put before the lane either.
#[tokio::test]
async fn a_commit_is_acknowledged_while_a_lane_items_put_is_in_flight() {
    let fixture = Fixture::new(|builder| builder).await;
    fixture.manifest_gate.block_next();
    let changing = fixture.namespace.clone();
    let change = tokio::spawn(async move {
        changing
            .set_retention_window(Some(retention_window(0)))
            .await
    });
    bounded(
        Duration::from_secs(10),
        fixture.manifest_gate.wait_until_blocked(),
    )
    .await
    .expect("the window change reaches its manifest put");
    bounded(
        Duration::from_secs(10),
        fixture
            .namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "during", "during",
            ))),
    )
    .await
    .expect("the commit is acknowledged while the window change's put is in flight")
    .expect("commit");
    fixture.manifest_gate.release();
    bounded(Duration::from_secs(10), change)
        .await
        .expect("the window change settles")
        .expect("join the window change")
        .expect("change the window");
    fixture.shut_down().await;
}

/// Checks the order of the session's locks under contention (the fold lock,
/// the fold permit, the publication lane, then the engine): a retention
/// advance that arrives during a fold puts its floor object and then waits
/// for the fold, a deletion that arrives during a fold waits for it, the
/// commits that arrive meanwhile are acknowledged, and each operation
/// finishes once the fold's put returns.
#[tokio::test]
async fn an_advance_a_deletion_and_commits_finish_beside_a_fold() {
    let clock = Arc::new(ManualClock::at_system_time());
    let fixture = Fixture::new({
        let clock = Arc::clone(&clock);
        move |builder| builder.wall_clock(clock)
    })
    .await;
    fixture
        .namespace
        .fold_wal()
        .await
        .expect("fold the first commit");
    fixture.writer.drain().await.expect("drain");
    // Every commit is a grace window old, so the advance can pass them.
    clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
    for (round, deletes) in [(0, false), (1, true)] {
        let commit = |name: &str| {
            fixture
                .namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(name, name)))
        };
        commit(&format!("tail-{round}"))
            .await
            .expect("a commit for the fold");
        fixture.manifest_gate.block_next();
        let folding = fixture.namespace.clone();
        let fold = tokio::spawn(async move { folding.fold_wal().await });
        bounded(
            Duration::from_secs(10),
            fixture.manifest_gate.wait_until_blocked(),
        )
        .await
        .expect("the fold reaches its manifest put");
        let other = fixture.namespace.clone();
        let mut operation = tokio::spawn(async move {
            if deletes {
                other.delete().await.map(drop)
            } else {
                other.advance_retention_floor().await.map(drop)
            }
        });
        if !deletes {
            bounded(Duration::from_secs(10), commit(&format!("during-{round}")))
                .await
                .expect("a commit is acknowledged while the fold's put is in flight")
                .expect("commit");
            bounded(Duration::from_secs(10), async {
                while !fixture.has_floor_object().await {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the advance puts its floor object while the fold's put is in flight");
        }
        assert_matches!(
            bounded(Duration::from_millis(200), &mut operation).await,
            Err(_),
            "the operation waits for the fold"
        );
        fixture.manifest_gate.release();
        assert_matches!(
            bounded(Duration::from_secs(10), fold).await,
            Ok(Ok(Ok(_))),
            "the fold finishes"
        );
        assert_matches!(
            bounded(Duration::from_secs(10), operation).await,
            Ok(Ok(Ok(()))),
            "the operation finishes after the fold"
        );
    }
    fixture.shut_down().await;
}

/// Checks that two units that both need a claim claim once: a commit of a
/// new session finds the session without its writer epoch and waits for the
/// publication lane, which a fold keeps while it claims, and once the commit
/// has the lane and the engine it finds the fold's claim and starts from
/// the position that the fold kept, without a load.
#[tokio::test]
async fn a_unit_that_waits_for_a_claim_starts_from_the_kept_position() {
    let fixture = Fixture::new(|builder| builder).await;
    let other = crate::LoonFs::builder_with_store(
        fixture.snapshot_gate.clone() as SharedStore,
        crate::Context::Service,
    )
    .writer_id(loonfs_test_support::ids::writer_id("writer-b"))
    .config(crate::RuntimeConfig {
        min_publish_interval_ms: 0,
        ..crate::RuntimeConfig::default()
    })
    .monotonic_timer(Arc::new(ManualClock::new(0)))
    .build()
    .await
    .expect("build the other writer");
    let namespace = other
        .open_namespace(&fixture.namespace_id)
        .expect("open a new session");
    let claim = fixture
        .current_manifest_no()
        .await
        .successor()
        .expect("next manifest number");
    let engine = namespace.session().publisher.lock_engine().await;
    let committing = namespace.clone();
    let mut commit = tokio::spawn(async move {
        committing
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "claimed", "claimed",
            )))
            .await
    });
    assert_matches!(
        bounded(Duration::from_millis(200), &mut commit).await,
        Err(_),
        "the commit waits for the engine"
    );
    let folding = namespace.clone();
    let mut fold = tokio::spawn(async move { folding.fold_wal().await });
    assert_matches!(
        bounded(Duration::from_millis(200), &mut fold).await,
        Err(_),
        "the fold keeps the lane and waits for the engine"
    );
    fixture.reset_requests();
    drop(engine);
    let folded = bounded(Duration::from_secs(10), fold)
        .await
        .expect("the fold settles")
        .expect("join the fold")
        .expect("fold");
    let committed = bounded(Duration::from_secs(10), commit)
        .await
        .expect("the commit settles")
        .expect("join the commit")
        .expect("the commit commits");
    assert!(
        folded.manifest_head_seq < committed.committed_seq,
        "the fold claimed, and the commit is above the fold's manifest"
    );
    let operations = fixture.store.snapshot();
    let fold_put = fixture.manifest(folded.manifest_no);
    let after_fold = operations
        .iter()
        .position(|operation| {
            matches!(operation, RecordedOperation::Put { .. }) && operation.key() == fold_put
        })
        .expect("the fold's manifest put");
    let wal = wal_prefix(&fixture.namespace_id);
    assert!(
        !operations[after_fold..].iter().any(|operation| {
            operation.key().starts_with(&wal) && !matches!(operation, RecordedOperation::Put { .. })
        }),
        "the commit reads no WAL object after the fold: {operations:?}"
    );
    assert_eq!(
        fixture.manifest_puts(),
        numbers_from(claim, 2),
        "one claim, then the fold"
    );
    other.shutdown().await.expect("shut down the other writer");
}
