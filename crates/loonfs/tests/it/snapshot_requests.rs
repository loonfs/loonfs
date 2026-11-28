//! Store requests of snapshot calls on a writer's runtime after a fold:
//! creating a snapshot, reading at it, and deleting it.

use loonfs::{
    CreateSnapshotOptions, FoldWalOutcome, LoonFs, Namespace, SharedObjectStore, Writable,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    request_lines, KeyPredicate, MemoryStore, RecordedOperation, RecordingStore,
};
use loonfs_types::{ChecksumAlgorithm, NamespaceId};
use std::assert_matches;
use std::num::NonZeroU64;
use std::sync::Arc;

/// Which fold leaves the namespace's WAL tail folded before the calls.
#[derive(Debug, Clone, Copy)]
enum Fold {
    /// The session's own fold, which runs after every commit because the
    /// session folds at one unfolded WAL object.
    Session,
    /// An explicit fold through the maintenance handle.
    Explicit,
}

struct Fixture {
    recording: Arc<RecordingStore<MemoryStore>>,
    writer: LoonFs<Writable>,
    namespace_id: NamespaceId,
    namespace: Namespace<Writable>,
}

/// Creates a writer runtime and a namespace with four committed files, folds
/// the WAL tail with `fold`, and waits until no fold or compaction is
/// running.
async fn fixture(fold: Fold) -> Fixture {
    let name = format!("snapshot-calls-{fold:?}").to_lowercase();
    let namespace_id = namespace_id(&name);
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let builder = LoonFs::builder_with_store(store, loonfs::Context::Service)
        .writer_id(writer_id(&name))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .monotonic_timer(Arc::new(ManualClock::new(1_000_000)));
    let builder = match fold {
        Fold::Session => builder.session_fold_at_wal_objects(NonZeroU64::MIN),
        Fold::Explicit => builder,
    };
    let writer = builder.build().await.expect("writer");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    for index in 0..4 {
        let path = format!("/doc-{index}");
        let put = namespace.put_file(&path, b"doc", &actor);
        put.await.expect("put");
    }
    if let Fold::Explicit = fold {
        let maintenance = writer.clone();
        let folded = maintenance
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .fold_wal()
            .await
            .expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
    }
    writer.drain().await.expect("the fold settles");
    Fixture {
        recording,
        writer,
        namespace_id,
        namespace,
    }
}

impl Fixture {
    /// Checks that the calls since the last reset sent `expected` requests.
    fn assert_requests(&self, call: &str, expected: usize) {
        let operations = self.recording.take();
        assert_eq!(operations.len(), expected, "{call}: {operations:?}");
    }
}

/// Checks that after a fold, each snapshot creation on the writer's runtime
/// costs 3 store requests, each read at a snapshot costs 3, and each
/// deletion costs 2, whichever fold folded the tail.
///
/// A creation folds from the session's position and finds nothing to fold,
/// so it writes the snapshot and then verifies it: it checks that the fold's
/// manifest is still the newest with one HEAD for a successor and a read of
/// the manifest boundary (3 requests). The verification reads no manifest,
/// so the first creation after an explicit fold costs the same.
/// A read at a snapshot checks the writer position's manifest with one HEAD
/// for a successor and a read of the manifest boundary, and loads the
/// snapshot, and the segments of the
/// snapshot's manifest are cached. A deletion loads the snapshot and deletes it.
#[tokio::test]
async fn snapshot_calls_after_a_fold_cost_a_snapshot_record_and_its_verification() {
    for fold in [Fold::Session, Fold::Explicit] {
        let fixture = fixture(fold).await;
        let mut snapshots = Vec::new();
        for index in 0..3 {
            fixture.recording.reset();
            let options = CreateSnapshotOptions {
                name: Some(format!("admin-{index}")),
                ..CreateSnapshotOptions::default()
            };
            let created = fixture
                .namespace
                .create_snapshot_with_options(&options)
                .await;
            snapshots.push(created.expect("create").snapshot_id);
            fixture.namespace.wait_for_fold().await.expect("settle");
            fixture.assert_requests(&format!("{fold:?}: create {index}"), 3);
        }
        let reader = fixture.namespace.read_only();
        for (index, snapshot) in snapshots.iter().enumerate() {
            fixture.recording.reset();
            let view = reader.read_view_at_snapshot(snapshot).await;
            let stat = view.expect("view").stat(&format!("/doc-{index}")).await;
            stat.expect("stat");
            fixture.assert_requests(&format!("{fold:?}: read at {index}"), 3);
        }
        for (index, snapshot) in snapshots.iter().enumerate() {
            fixture.recording.reset();
            let deleted = fixture.namespace.delete_snapshot(snapshot);
            deleted.await.expect("delete");
            fixture.assert_requests(&format!("{fold:?}: delete {index}"), 2);
        }
    }
}

/// Checks that on the writer's runtime, whose position was checked within
/// the revalidation bound, a snapshot retrieval costs one request, the read
/// of the snapshot's record, and a snapshot listing costs one listing of the
/// snapshots and one read of each record. Neither call reads a manifest or
/// the manifest boundary, because both take the writer position's head.
#[tokio::test]
async fn snapshot_lookups_on_the_writers_runtime_read_snapshot_records_and_no_manifest() {
    let fixture = fixture(Fold::Session).await;
    let mut snapshots = Vec::new();
    for index in 0..3 {
        let options = CreateSnapshotOptions {
            name: Some(format!("lookup-{index}")),
            ..CreateSnapshotOptions::default()
        };
        let created = fixture
            .namespace
            .create_snapshot_with_options(&options)
            .await;
        snapshots.push(created.expect("create"));
        fixture.namespace.wait_for_fold().await.expect("settle");
    }
    let snapshot_prefix = loonfs_objectstore::keys::snapshot_prefix(&fixture.namespace_id);
    let reader = fixture.namespace.read_only();
    for (index, snapshot) in snapshots.iter().enumerate() {
        fixture.recording.reset();
        let read = reader.get_snapshot(&snapshot.snapshot_id).await;
        assert_eq!(&read.expect("get"), snapshot);
        let operations = fixture.recording.take();
        assert_matches!(
            &operations[..],
            [RecordedOperation::GetWithMetadata { key, .. }] if key.starts_with(&snapshot_prefix),
            "get {index}"
        );
    }

    snapshots.sort_by(|left, right| left.snapshot_id.cmp(&right.snapshot_id));
    fixture.recording.reset();
    let mut pager = reader.list_snapshots();
    let page = pager.next().await.expect("one page").expect("list");
    assert!(pager.next().await.is_none());
    assert_eq!(page.snapshots, snapshots);
    let operations = fixture.recording.take();
    assert_eq!(operations.len(), 4, "list: {operations:?}");
    assert_matches!(
        &operations[0],
        RecordedOperation::List { prefix } if prefix == &snapshot_prefix
    );
    for operation in &operations[1..] {
        assert_matches!(
            operation,
            RecordedOperation::GetWithMetadata { key, .. } if key.starts_with(&snapshot_prefix)
        );
    }
}

/// Checks that creating a snapshot of a namespace with 32 fork snapshots costs
/// the same 3 store requests as creating one with none, so the cost doesn't
/// grow with the number of snapshots. The forks keep the runtime's view of
/// the source, so the snapshot after them starts warm, like the ones before.
///
/// (A count of the namespace's live snapshots before and after the snapshot
/// write, which loads every snapshot twice, would add at least 64 requests
/// to this call.)
#[tokio::test]
async fn a_snapshot_of_a_namespace_with_32_fork_snapshots_costs_3_requests() {
    for fold in [Fold::Session, Fold::Explicit] {
        let fixture = fixture(fold).await;
        let actor = loonfs_test_support::test_actor();
        for index in 0..32 {
            let target = namespace_id(&format!("fork-{fold:?}-{index}").to_lowercase());
            let forked = fixture
                .writer
                .fork_namespace(&fixture.namespace_id, &target, &actor);
            forked.await.expect("fork");
        }
        fixture.namespace.wait_for_fold().await.expect("settle");
        fixture.recording.reset();
        let options = CreateSnapshotOptions {
            name: Some("forked".to_owned()),
            ..CreateSnapshotOptions::default()
        };
        let created = fixture
            .namespace
            .create_snapshot_with_options(&options)
            .await;
        created.expect("create");
        fixture.namespace.wait_for_fold().await.expect("settle");
        fixture.assert_requests(&format!("{fold:?}: create with 32 fork snapshots"), 3);
    }
}

/// Returns a store and a namespace with one committed file, written by a
/// writer runtime that keeps its session open, and that runtime.
async fn quiet_writer(
    name: &str,
    fold: bool,
) -> (
    Arc<RecordingStore<MemoryStore>>,
    NamespaceId,
    LoonFs<Writable>,
    Namespace<Writable>,
) {
    let namespace_id = namespace_id(name);
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = loonfs::test_support::writer(store, &format!("{name}-writer")).await;
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    namespace
        .put_file("/doc", b"doc", &actor)
        .await
        .expect("put");
    if fold {
        namespace.fold_wal().await.expect("fold");
    }
    writer.drain().await.expect("the writer settles");
    (recording, namespace_id, writer, namespace)
}

/// Checks that a snapshot in a runtime whose session has no position, of a
/// namespace whose tail has no commit, writes its record on the current
/// manifest without a fold or a claim. It pins behaviour that this change
/// keeps, and it passes before the change too. The pinned requests are the
/// discovery of the head (a read of the boundary, the manifest listing, a
/// GET of the newest manifest, and a second read of the boundary), the GET
/// of the next WAL number that finds the tail empty, the check of the
/// manifest (a HEAD of the next manifest number and a read of the
/// boundary), the record's put, and the verification's HEAD of the next
/// manifest number and read of the boundary.
#[tokio::test]
async fn a_snapshot_with_no_commit_in_its_tail_writes_its_record_without_a_claim() {
    let (recording, namespace_id, _writer, _held) = quiet_writer("snapshot-no-tail", true).await;
    let store: SharedObjectStore = recording.clone();
    let other = loonfs::test_support::writer(store.clone(), "snapshot-no-tail-other").await;
    let session = other.open_namespace(&namespace_id).expect("open");
    let epoch = crate::common::writer_epoch(&store, &namespace_id).await;
    recording.reset();
    session.create_snapshot().await.expect("snapshot");
    let operations = recording.take();
    assert!(
        operations.iter().all(|operation| {
            !matches!(operation, RecordedOperation::Put { key, .. }
                if key.contains("/manifests/") || key.contains("/wal/"))
        }),
        "the snapshot puts no manifest and no WAL object: {operations:?}"
    );
    // The find of the current manifest, the check that the tail after it is
    // empty, the record, and the record's verification against the next
    // manifest number.
    assert_eq!(
        request_lines(&operations),
        [
            "get snapshot-no-tail Boundary",
            "list namespaces/snapshot-no-tail/manifests/",
            "get snapshot-no-tail MetadataManifest 00000000000000000004",
            "get snapshot-no-tail Boundary",
            "get snapshot-no-tail WalObject 00000000000000000003",
            "head snapshot-no-tail MetadataManifest 00000000000000000005",
            "get snapshot-no-tail Boundary",
            "put snapshot-no-tail Snapshot",
            "head snapshot-no-tail MetadataManifest 00000000000000000005",
            "get snapshot-no-tail Boundary",
        ],
        "{operations:?}"
    );
    assert_eq!(
        crate::common::writer_epoch(&store, &namespace_id).await,
        epoch,
        "the snapshot claims nothing"
    );
}

/// Checks that a snapshot in a runtime whose session has no position, of a
/// namespace whose tail has a commit, claims the namespace and folds the
/// tail as its writer, so the snapshot's manifest covers the commit, and the
/// session that wrote the commit is displaced.
#[tokio::test]
async fn a_snapshot_with_a_commit_in_its_tail_claims_and_folds() {
    let (recording, namespace_id, _writer, held) = quiet_writer("snapshot-tail", false).await;
    let store: SharedObjectStore = recording.clone();
    let other = loonfs::test_support::writer(store.clone(), "snapshot-tail-other").await;
    let epoch = crate::common::writer_epoch(&store, &namespace_id).await;
    let snapshot = other
        .open_namespace(&namespace_id)
        .expect("open")
        .create_snapshot()
        .await
        .expect("snapshot");
    assert_eq!(
        crate::common::writer_epoch(&store, &namespace_id).await,
        epoch + 1,
        "the snapshot's session claims the namespace"
    );
    assert_eq!(snapshot.captured_seq, loonfs_types::CommitSeq(1));
    let fenced = held
        .put_file("/later", b"later", &loonfs_test_support::test_actor())
        .await
        .expect_err("the displaced session's next commit");
    assert_eq!(fenced.code(), loonfs::ErrorCode::WriterFenced);
}

/// Returns a runtime whose snapshot records, before each put, get a newer
/// manifest of `namespace_id` from the namespace's writer for the next
/// `refusals` records, and the store that publishes them.
async fn refusing_runtime(
    recording: &Arc<RecordingStore<MemoryStore>>,
    namespace_id: &NamespaceId,
    name: &str,
    refusals: usize,
) -> (
    LoonFs<Writable>,
    Arc<loonfs_test_support::stores::NewerManifestStore<Arc<RecordingStore<MemoryStore>>>>,
) {
    let rival: SharedObjectStore = recording.clone();
    let publishing = Arc::new(loonfs_test_support::stores::NewerManifestStore::new(
        recording.clone(),
        rival,
        namespace_id.clone(),
        KeyPredicate::prefix(loonfs_objectstore::keys::snapshot_prefix(namespace_id)),
        loonfs_test_support::stores::OperationSelector::PutCreateIfAbsent,
    ));
    let runtime = loonfs::test_support::writer(publishing.clone(), name).await;
    publishing.arm_for(refusals);
    (runtime, publishing)
}

/// Checks that a snapshot of a namespace with no commit in its tail, whose
/// first record a newer manifest refuses, takes its second record at the
/// newer manifest, claims nothing, and lists only that record. It pins
/// behaviour that this change keeps, and it passes before the change too.
///
/// The namespace's writer runs in another runtime and publishes a manifest
/// that folds no WAL object, as its compaction or window change does, just
/// before the first record's put.
#[tokio::test]
async fn a_snapshot_refused_once_succeeds_at_the_newer_manifest() {
    let (recording, namespace_id, _writer, _held) =
        quiet_writer("snapshot-refused-once", true).await;
    let store: SharedObjectStore = recording.clone();
    let epoch = crate::common::writer_epoch(&store, &namespace_id).await;
    let (other, publishing) =
        refusing_runtime(&recording, &namespace_id, "snapshot-refused-once-other", 1).await;
    let snapshot = other
        .open_namespace(&namespace_id)
        .expect("open")
        .create_snapshot()
        .await
        .expect("the second record is verified");
    assert_eq!(publishing.published(), 1);
    let newest = loonfs_core::test_support::namespace_store(store.clone(), &namespace_id)
        .load_current_manifest()
        .await
        .expect("the newest manifest")
        .manifest()
        .manifest_no;
    assert_eq!(snapshot.snapshot_id.manifest_no(), newest);
    let listed = other
        .read_only()
        .namespace(&namespace_id)
        .list_snapshots()
        .page()
        .await
        .expect("list the snapshots");
    assert_eq!(listed.snapshots.len(), 1, "{listed:?}");
    assert_eq!(
        crate::common::writer_epoch(&store, &namespace_id).await,
        epoch,
        "the snapshot claims nothing"
    );
}

/// Checks that a snapshot of a namespace with no commit in its tail, whose
/// record a newer manifest refuses at each verification, gives up after
/// `MAX_CONTENTION_ATTEMPTS` (8) records with `store_contention`, marks
/// each refused record deleted, so no snapshot is listed, and claims
/// nothing. It pins behaviour that this change keeps, and it passes before
/// the change too.
///
/// The namespace's writer runs in another runtime and publishes a manifest
/// that folds no WAL object, as its compaction or window change does, just
/// before each record's put, so each record's verification finds that the
/// manifest it was taken at is no longer current.
#[tokio::test]
async fn a_snapshot_refused_eight_times_returns_store_contention() {
    use loonfs_objectstore::ObjectStoreExtension;
    let (recording, namespace_id, _writer, _held) = quiet_writer("snapshot-refused", true).await;
    let store: SharedObjectStore = recording.clone();
    let epoch = crate::common::writer_epoch(&store, &namespace_id).await;
    let (other, publishing) = refusing_runtime(
        &recording,
        &namespace_id,
        "snapshot-refused-other",
        usize::MAX,
    )
    .await;
    let error = other
        .open_namespace(&namespace_id)
        .expect("open")
        .create_snapshot()
        .await
        .expect_err("every record is refused");
    assert_eq!(
        error.code(),
        loonfs::ErrorCode::StoreContention,
        "{error:?}"
    );
    assert_eq!(
        publishing.published(),
        loonfs_core::limits::MAX_CONTENTION_ATTEMPTS,
        "one record per attempt"
    );
    let listed = other
        .read_only()
        .namespace(&namespace_id)
        .list_snapshots()
        .page()
        .await
        .expect("list the snapshots");
    assert!(
        listed.snapshots.is_empty(),
        "every refused record is marked deleted: {listed:?}"
    );
    assert_eq!(
        recording
            .list_prefix(&loonfs_objectstore::keys::snapshot_prefix(&namespace_id))
            .await
            .expect("list the snapshot records")
            .len(),
        loonfs_core::limits::MAX_CONTENTION_ATTEMPTS,
        "each attempt wrote one record"
    );
    assert_eq!(
        crate::common::writer_epoch(&store, &namespace_id).await,
        epoch,
        "the snapshot claims nothing"
    );
}

/// What another runtime does once, before the first snapshot record's put
/// that the interceptor sees.
type BeforeRecord =
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()> + Send + Sync + 'static>;

/// Runs a step of another runtime before the first snapshot record's put.
struct ActBeforeRecord {
    prefix: String,
    act: tokio::sync::Mutex<Option<BeforeRecord>>,
}

impl std::fmt::Debug for ActBeforeRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActBeforeRecord")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl loonfs_test_support::stores::Interceptor for ActBeforeRecord {
    type Guard = ();

    async fn before(
        &self,
        context: &loonfs_test_support::stores::OperationContext<'_>,
    ) -> (loonfs_test_support::stores::Intercept, ()) {
        if context.key().starts_with(&self.prefix)
            && loonfs_test_support::stores::OperationSelector::PutCreateIfAbsent
                .matches(context.kind())
        {
            let act = self.act.lock().await.take();
            if let Some(act) = act {
                act().await;
            }
        }
        (loonfs_test_support::stores::Intercept::Continue, ())
    }
}

/// Returns a runtime over `recording` that runs `act` before its first
/// snapshot record's put of `namespace_id`.
async fn runtime_acting_before_record(
    recording: &Arc<RecordingStore<MemoryStore>>,
    namespace_id: &NamespaceId,
    name: &str,
    act: BeforeRecord,
) -> LoonFs<Writable> {
    let store = Arc::new(
        loonfs_test_support::stores::InterceptStore::with_interceptor(
            recording.clone(),
            ActBeforeRecord {
                prefix: loonfs_objectstore::keys::snapshot_prefix(namespace_id),
                act: tokio::sync::Mutex::new(Some(act)),
            },
        ),
    );
    loonfs::test_support::writer(store, name).await
}

/// Checks that a snapshot whose first record a newer manifest refuses, and
/// whose retry then finds a commit in the tail, claims the namespace, folds
/// the tail, and takes the snapshot at the fold, which covers the commit.
///
/// The namespace's writer runs in another runtime. Before the first
/// record's put, it commits and folds, which refuses the record, and commits
/// again, so the retry finds that commit in the tail.
#[tokio::test]
async fn a_snapshot_whose_retry_finds_a_commit_in_its_tail_claims_and_folds() {
    let (recording, namespace_id, _writer, held) = quiet_writer("snapshot-retry-tail", true).await;
    let store: SharedObjectStore = recording.clone();
    let epoch = crate::common::writer_epoch(&store, &namespace_id).await;
    let writer_session = held.clone();
    let other = runtime_acting_before_record(
        &recording,
        &namespace_id,
        "snapshot-retry-tail-other",
        Box::new(move || {
            Box::pin(async move {
                let actor = loonfs_test_support::test_actor();
                writer_session
                    .put_file("/refusing", b"refusing", &actor)
                    .await
                    .expect("the writer commits");
                writer_session.fold_wal().await.expect("the writer folds");
                writer_session
                    .put_file("/tail", b"tail", &actor)
                    .await
                    .expect("the writer commits into the tail");
            })
        }),
    )
    .await;
    let snapshot = other
        .open_namespace(&namespace_id)
        .expect("open")
        .create_snapshot()
        .await
        .expect("the retry's snapshot");
    assert_eq!(
        crate::common::writer_epoch(&store, &namespace_id).await,
        epoch + 1,
        "the retry claims to fold the tail"
    );
    assert_eq!(
        snapshot.captured_seq,
        loonfs_types::CommitSeq(3),
        "the snapshot covers the tail's commit"
    );
    let listed = other
        .read_only()
        .namespace(&namespace_id)
        .list_snapshots()
        .page()
        .await
        .expect("list the snapshots");
    assert_eq!(
        listed.snapshots.len(),
        1,
        "the refused record is marked deleted"
    );
}

/// Checks that a snapshot whose verification finds another writer's claim
/// takes its snapshot again: the claim refuses the first record, and the
/// retry finds the claiming writer's commit in the tail, so it claims the
/// namespace back, folds, and takes the snapshot at the fold.
#[tokio::test]
async fn a_snapshot_whose_verification_finds_another_writers_claim_takes_it_again() {
    let (recording, namespace_id, _writer, _held) = quiet_writer("snapshot-claimed", true).await;
    let store: SharedObjectStore = recording.clone();
    let rival = loonfs::test_support::writer(store.clone(), "snapshot-claimed-rival").await;
    let rival_session = rival.open_namespace(&namespace_id).expect("open the rival");
    let other = runtime_acting_before_record(
        &recording,
        &namespace_id,
        "snapshot-claimed-other",
        Box::new(move || {
            Box::pin(async move {
                rival_session
                    .put_file("/rival", b"rival", &loonfs_test_support::test_actor())
                    .await
                    .expect("the rival claims and commits");
            })
        }),
    )
    .await;
    let snapshot = other
        .open_namespace(&namespace_id)
        .expect("open")
        .create_snapshot()
        .await
        .expect("the retry's snapshot");
    assert_eq!(
        snapshot.captured_seq,
        loonfs_types::CommitSeq(2),
        "the snapshot covers the rival's commit"
    );
    let head = loonfs_core::test_support::namespace_store(store.clone(), &namespace_id)
        .load_read_state()
        .await
        .expect("the head");
    assert_eq!(
        head.writer.map(|writer| writer.writer_id),
        Some(loonfs_test_support::ids::writer_id(
            "snapshot-claimed-other"
        )),
        "the retry's session claimed the namespace back"
    );
    rival.shutdown().await.expect("stop the rival");
}
