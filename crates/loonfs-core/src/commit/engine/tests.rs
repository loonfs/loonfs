//! Commit engine tests: admission weight, identity, ceilings, fencing, basis
//! checks, and publish-view caching.

mod content;
mod deletion;
mod inline;
mod merge;
mod preparation_panic;
mod retention_floor;
mod unknown_outcome;

use super::*;
use crate::cache::MetadataSegmentCache;
use crate::error::ErrorCode;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::storage::content_admission::{ContentTokenError, PreparedContent};
use crate::storage::inline_content::InlineContent;
use crate::test_support::{create_directory_request, WriterAt};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use futures::StreamExt;
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::format::wal::{
    MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
};
use loonfs_types::CommitId;
use loonfs_types::{CommitSeq, ContentRef, PrincipalId, PrincipalSet, Subject};
use loonfs_types::{ContentId, WriterEpoch};
use std::assert_matches;
use std::collections::BTreeSet;
use tempfile::tempdir;

fn context(writer_id: &str) -> WriterAt {
    crate::test_support::writer_at(writer_id, 1_000)
}

#[test]
fn admission_weight_includes_annotation_proofs_and_preparation_errors() {
    let request = create_directory_request("weight", "docs");
    let baseline = CommitCandidate::new(request.clone()).estimated_retained_bytes();
    let mut annotated = request.clone();
    annotated.message = Some(String::new());
    let empty_annotation = CommitCandidate::new(annotated.clone()).estimated_retained_bytes();
    annotated.message = Some("x".repeat(4096));
    assert!(CommitCandidate::new(annotated).estimated_retained_bytes() >= empty_annotation + 4096);
    let proof = PreparedContent::for_durable_content_write(
        NamespaceId::parse("demo").expect("namespace"),
        ContentRef::blob_v1(
            ContentId::generate(),
            b"proof",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        ),
    );
    let prepared = CommitCandidate::prepared(request.clone(), vec![proof; 100]);
    assert!(
        prepared.estimated_retained_bytes()
            > baseline + 100 * std::mem::size_of::<PreparedContent>()
    );
    let rejected = CommitCandidate::rejected(
        request,
        ContentPreparationError::ContentToken(vec![
            (
                ContentId::generate(),
                ContentTokenError::Codec("x".repeat(4096))
            );
            10
        ]),
    );
    assert!(rejected.estimated_retained_bytes() > baseline + 40960);
}

#[test]
fn principals_add_to_candidate_admission_weight() {
    let candidate = CommitCandidate::new(create_directory_request("subject-weight", "docs"));
    let before = candidate.estimated_retained_bytes();
    let principals = (0..64)
        .map(|index| {
            PrincipalId::parse(format!("{index:03}{}", "x".repeat(253))).expect("principal")
        })
        .collect::<BTreeSet<_>>();
    let candidate = candidate.with_subject(Subject {
        principals: PrincipalSet::new(principals).expect("principals"),
    });
    let after = candidate.estimated_retained_bytes();
    assert!(after >= before + 64 * 256);
}

#[test]
fn semantic_identity_excludes_content_preparation() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let request = create_directory_request("same-mutation", "docs");
    let ready = CommitCandidate::new(request.clone());
    let rejected = CommitCandidate::rejected(
        request,
        ContentPreparationError::ContentToken(vec![(
            ContentId::generate(),
            ContentTokenError::Expired,
        )]),
    );

    assert_eq!(
        ready.semantic_identity(&namespace_id),
        rejected.semantic_identity(&namespace_id)
    );
}

/// Returns a candidate that creates `operations` directories, with a message
/// of `message_bytes` bytes when `message_bytes` is set.
fn request_with(operations: usize, message_bytes: Option<usize>) -> CommitCandidate {
    CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("ceiling").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: message_bytes.map(|bytes| "m".repeat(bytes)),
        operations: (0..operations)
            .map(|index| FilesystemOperation::CreateDirectory {
                path: loonfs_types::AbsolutePath::parse(format!("/dir-{index}"))
                    .expect("valid path"),
                parents: false,
            })
            .collect(),
    })
}

#[test]
fn requests_past_the_operation_or_message_ceiling_are_rejected() {
    use crate::limits::{MAX_COMMIT_MESSAGE_BYTES, MAX_COMMIT_OPERATIONS};
    for (label, operations, message_bytes, admitted) in [
        ("operations past", MAX_COMMIT_OPERATIONS + 1, None, false),
        ("operations at", MAX_COMMIT_OPERATIONS, None, true),
        ("message past", 1, Some(MAX_COMMIT_MESSAGE_BYTES + 1), false),
        ("message at", 1, Some(MAX_COMMIT_MESSAGE_BYTES), true),
    ] {
        let expected = if admitted {
            Ok(())
        } else {
            Err(ErrorCode::InvalidRequest)
        };
        let checked = request_with(operations, message_bytes).validate_request_limits();
        assert_eq!(
            checked.map_err(|error| error.code()),
            expected,
            "{label} the ceiling"
        );
    }
}

fn create_dir(commit_id: &str, display_name: &str) -> CommitCandidate {
    CommitCandidate::new(create_directory_request(commit_id, display_name))
}

async fn wal_object_count(store: &LocalFsStore, namespace_id: &NamespaceId) -> usize {
    store
        .list_prefix_stream(&wal_prefix(namespace_id))
        .collect::<Vec<_>>()
        .await
        .len()
}

#[tokio::test]
async fn fencing_during_publish_view_load_still_reports_writer_fenced() {
    use loonfs_test_support::stores::{BlockingStore, KeyPredicate};
    use std::sync::Arc as StdArc;

    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer_a = context("writer-a");

    // Block the WAL tail read: it sits between the read state that the
    // fence check uses and the closing etag recheck.
    let store = StdArc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::Read,
    ));

    crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &writer_a)
        .create()
        .await
        .expect("bootstrap");

    // Engine A reads through the blocking store; nothing blocks until
    // `block_next` arms it.
    let mut engine_a = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(StdArc::clone(&store), &namespace_id),
        writer_a.writer_id.clone(),
    );
    engine_a
        .publish_batch(vec![create_dir("from-a-first", "alpha")], &writer_a.clock())
        .await
        .results[0]
        .as_ref()
        .expect("writer a first commit");
    // Force a fresh manifest load on the next publish.
    engine_a.invalidate_projection();

    store.block_next();
    let publish_a = tokio::spawn(async move {
        let mut engine = engine_a;
        let result = engine
            .publish_batch(
                vec![create_dir("from-a-second", "gamma")],
                &writer_a.clock(),
            )
            .await;
        result.results[0].as_ref().err().map(|error| error.code())
    });

    // A has read a head that still names it. Writer B takes the
    // epoch while A is parked mid-load, so A is fenced by the time it
    // rechecks the etag.
    store.wait_until_blocked().await;
    let writer_b = context("writer-b");
    let mut engine_b = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store.inner(), &namespace_id),
        writer_b.writer_id.clone(),
    );
    engine_b
        .publish_batch(vec![create_dir("from-b-first", "beta")], &writer_b.clock())
        .await
        .results[0]
        .as_ref()
        .expect("writer b takeover commit");
    store.release();

    let code = publish_a.await.expect("join publish a");
    assert_eq!(code, Some(ErrorCode::WriterFenced));
}

/// Checks that a commit that finds a segment of its position's manifest
/// missing, and can't load the current manifest to check whether a newer
/// manifest replaced its position's manifest, returns the load's error as
/// the error of the operation that read the segment, instead of
/// `namespace_corrupt`.
#[tokio::test]
async fn a_commit_that_cannot_check_its_manifest_returns_the_load_error() {
    use loonfs_test_support::stores::{FailStore, InjectedError};

    let temp_dir = tempdir().expect("tempdir");
    let base = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&base, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let failing = FailStore::new(
        base.clone(),
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace_id,
        )),
        OperationSelector::List,
        InjectedError::Transport("listing failed".to_owned()),
    );
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&failing, &namespace_id),
        writer.writer_id.clone(),
    );
    engine
        .publish_batch(vec![create_dir("first", "alpha")], &writer.clock())
        .await
        .results
        .remove(0)
        .expect("first commit");
    let folded = crate::test_support::namespace_store(&failing, &namespace_id)
        .fold_wal_tail(engine.writer_position(), &writer.clock())
        .await
        .expect("fold");
    engine.record_manifest_published(folded.current);
    let current = crate::test_support::namespace_store(&base, &namespace_id)
        .load_current_manifest()
        .await
        .expect("the position's manifest");
    for run in &current.payload().runs {
        for segment in &run.segments {
            base.delete(&loonfs_objectstore::keys::metadata_segment_object_key(
                segment,
            ))
            .await
            .expect("delete a segment");
        }
    }
    failing.fail_next(1);
    let error = engine
        // A directory named as the folded one, so the commit reads that
        // name's rows from the position's segments.
        .publish_batch(vec![create_dir("second", "alpha")], &writer.clock())
        .await
        .results
        .remove(0)
        .expect_err("the commit finds a segment missing");
    assert_matches!(
        error,
        CoreError::FailedOperation { operation_index: 0, error }
            if matches!(*error, CoreError::Store { .. }),
        "{error:?}"
    );
}

/// Checks that a writer session that loads a stored manifest with a writer
/// epoch below its own returns `namespace_corrupt`, at each entry that loads
/// the session's writer anchor: a commit, a retention advance through the
/// session, a deletion, and the fence that a session owes after a WAL put
/// with an unknown outcome. A lower epoch can't follow the session's own
/// claim in a correct store, so the error doesn't mark the session fenced.
#[tokio::test]
async fn a_stored_manifest_below_the_sessions_writer_epoch_is_corrupt() {
    for entry in ["commit", "retention advance", "deletion", "fence"] {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer)
            .create()
            .await
            .expect("bootstrap");
        let mut engine = NamespaceCommitEngine::new(
            crate::test_support::namespace_store(&store, &namespace_id),
            writer.writer_id.clone(),
        );
        engine
            .publish_batch(vec![create_dir("first", "alpha")], &writer.clock())
            .await
            .results
            .remove(0)
            .expect("the session's first commit acquires its epoch");
        let acquired = match &*engine.position.lock_session() {
            WriterSessionStatus::Acquired(acquired) => acquired.clone(),
            other => panic!("{entry}: the session holds its epoch: {other:?}"),
        };
        let current = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("the session's manifest");
        let mut lower = current.payload().clone();
        lower.manifest_no = lower.manifest_no.successor().expect("next manifest number");
        lower.writer_epoch = WriterEpoch(acquired.writer_epoch.0 - 1);
        lower.writer = None;
        store
            .put(
                &loonfs_objectstore::keys::metadata_manifest_object(
                    &namespace_id,
                    &lower.manifest_no,
                ),
                crate::manifest::publish::encode_manifest(lower)
                    .into_bytes()
                    .into(),
                loonfs_objectstore::PutMode::CreateIfAbsent,
            )
            .await
            .expect("put a manifest with a lower writer epoch");
        // Without a position, the session's next unit loads its writer
        // anchor from the store.
        engine.invalidate_projection();
        if entry == "fence" {
            *engine.position.lock_session() =
                WriterSessionStatus::HoldsEpochOwesFence(acquired.clone());
        }
        let error = match entry {
            "retention advance" => engine
                .publish_floor_object(&current, CommitSeq(1), &writer.clock())
                .await
                .expect_err("the advance finds the lower epoch"),
            "deletion" => engine
                .delete_namespace(
                    &crate::test_support::administrator_of(&store, &namespace_id),
                    Default::default(),
                )
                .await
                .expect_err("the deletion finds the lower epoch"),
            _ => engine
                .publish_batch(vec![create_dir("second", "beta")], &writer.clock())
                .await
                .results
                .remove(0)
                .expect_err("the commit finds the lower epoch"),
        };
        assert_eq!(
            error.code(),
            ErrorCode::NamespaceCorrupt,
            "{entry}: {error:?}"
        );
        assert!(
            !matches!(
                *engine.position.lock_session(),
                WriterSessionStatus::Fenced(_)
            ),
            "{entry}: a corrupt store doesn't fence the session"
        );
    }
}

/// Checks that after a takeover, the old session's next publish returns
/// `store_contention` and every later one returns `writer_fenced`, and that the
/// head keeps the new writer's epoch.
#[tokio::test]
async fn a_fenced_session_stays_fenced_and_leaves_the_head_alone() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer_a = context("writer-a");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer_a)
        .create()
        .await
        .expect("bootstrap");

    let mut engine_a = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer_a.writer_id.clone(),
    );
    engine_a
        .publish_batch(vec![create_dir("from-a-first", "alpha")], &writer_a.clock())
        .await
        .results
        .remove(0)
        .expect("writer a first commit");

    let writer_b = context("writer-b");
    let mut engine_b = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer_b.writer_id.clone(),
    );
    engine_b
        .publish_batch(vec![create_dir("from-b-first", "beta")], &writer_b.clock())
        .await
        .results
        .remove(0)
        .expect("writer b takeover commit");

    for expected in [ErrorCode::StoreContention, ErrorCode::WriterFenced] {
        let result = engine_a
            .publish_batch(
                vec![create_dir("from-a-second", "gamma")],
                &writer_a.clock(),
            )
            .await;
        assert_eq!(
            result.results[0]
                .as_ref()
                .expect_err("displaced writer")
                .code(),
            expected
        );
    }
    let epoch_after_fencing = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("read head")
        .writer_epoch;

    let still_fenced = engine_a
        .publish_batch(vec![create_dir("from-a-third", "delta")], &writer_a.clock())
        .await;
    let error = still_fenced.results[0]
        .as_ref()
        .expect_err("the session stays fenced");
    assert_eq!(error.code(), ErrorCode::WriterFenced);
    let head = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("read head");
    assert_eq!(head.writer_epoch, epoch_after_fencing);
    assert_eq!(
        head.writer.expect("writer block").writer_id.as_str(),
        "writer-b"
    );
}

/// A quiet period much longer than the time between a steady writer's
/// commits.
const QUIET_HOUR_MS: u64 = 60 * 60 * 1000;

/// Returns whether `operation` is a put-if-absent of a WAL object.
fn is_wal_put(operation: &RecordedOperation, namespace_id: &NamespaceId) -> bool {
    matches!(
        operation,
        RecordedOperation::Put {
            key,
            mode: loonfs_objectstore::PutMode::CreateIfAbsent,
            ..
        } if key.starts_with(&wal_prefix(namespace_id))
    )
}

/// Checks that a commit after a quiet hour sends its WAL put and no other
/// request: the put doesn't wait for a check of the position's tip or basis,
/// however old the position is.
#[tokio::test]
async fn a_commit_after_a_quiet_hour_sends_only_its_wal_put() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let tip = engine
        .writer_position()
        .expect("a position")
        .read_basis()
        .head
        .wal_no;

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    publish_one(&mut engine, &writer, "beta").await;
    let operations = store.take();
    assert!(
        matches!(operations.as_slice(), [put] if is_wal_put(put, &namespace_id)),
        "{operations:?}"
    );
    assert_eq!(
        engine
            .writer_position()
            .expect("a position")
            .read_basis()
            .head
            .wal_no
            .0,
        tip.0 + 1
    );
}

async fn publish_one<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine<S>,
    writer: &WriterAt,
    name: &str,
) -> NamespaceCommitEnginePublishOutcome {
    let deadline = OperationClock::start(writer.now_ms, Arc::clone(&engine.namespace.timer));
    let published = engine
        .publish_batch(vec![create_dir(name, name)], &deadline)
        .await;
    published.results[0].as_ref().expect(name);
    published
}

type GatedStore = RecordingStore<BlockingStore<LocalFsStore>>;

/// Records every operation and gates HEADs of `gated` keys.
fn gated_store(directory: &tempfile::TempDir, gated: KeyPredicate) -> GatedStore {
    RecordingStore::new(
        BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            gated,
            OperationSelector::Head,
        ),
        KeyPredicate::any(),
    )
}

/// A writer on a manual clock, with the segment cache a runtime gives every
/// engine, whose first publish loaded its view and committed.
async fn writer_after_one_publish<'a>(
    store: &'a GatedStore,
    namespace_id: &NamespaceId,
) -> (
    NamespaceCommitEngine<&'a GatedStore>,
    Arc<loonfs_test_support::clock::ManualClock>,
    WriterAt,
) {
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(store, namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: timer.clone(),
            segment_cache: Arc::new(MetadataSegmentCache::unshared(usize::MAX)),
            ..crate::test_support::namespace_store(store, namespace_id)
        },
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, &writer, "alpha").await;
    (engine, timer, writer)
}

/// Checks that a quiet writer whose next WAL number another process took
/// with a floor object collides with the floor object, drops its position,
/// and runs discovery on its next publication, which writes the commit
/// above the floor object, against the new floor.
#[tokio::test]
async fn a_quiet_writer_whose_next_number_is_a_floor_object_commits_above_it() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    for name in ["beta", "gamma", "delta"] {
        publish_one(&mut engine, &writer, name).await;
    }
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let tip = engine
        .writer_position()
        .expect("a position")
        .read_basis()
        .head
        .wal_no;
    let planned = namespace.load_current_manifest().await.expect("planned");
    namespace
        .publish_floor_object(&planned, CommitSeq(1))
        .await
        .expect("another process creates a floor object");

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    let deadline = OperationClock::start(writer.now_ms, Arc::clone(&engine.namespace.timer));
    let collided = engine
        .publish_batch(vec![create_dir("zeta", "zeta")], &deadline)
        .await;
    assert_matches!(
        collided.results.as_slice(),
        [Err(CoreError::WalPublish(
            crate::wal::WalPublishError::NumberTaken
        ))]
    );
    let operations = store.take();
    assert!(
        matches!(operations.as_slice(), [put] if is_wal_put(put, &namespace_id)),
        "the put is the only request before the collision: {operations:?}"
    );
    assert!(engine.writer_position().is_none());

    let retried = engine
        .publish_batch(vec![create_dir("zeta", "zeta")], &deadline)
        .await;
    retried.results[0].as_ref().expect("the retry commits");
    let position = engine.writer_position().expect("a position");
    assert_eq!(position.read_basis().head.wal_no.0, tip.0 + 2);
    assert_eq!(position.retention_floor_seq(), CommitSeq(1));
}

/// Checks that a writer that commits every few minutes for an hour, against
/// a manifest that nothing replaces, sends its WAL put and no other request
/// on each commit, so it never checks its tip and never reads its WAL tail
/// again.
#[tokio::test]
async fn a_steady_writer_sends_only_its_wal_puts() {
    const HOUR_MS: u64 = 60 * 60 * 1000;
    for spacing_ms in [4 * 60 * 1000, 6 * 60 * 1000] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = gated_store(&directory, KeyPredicate::new(|_| false));
        let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
        for commit in 1..=HOUR_MS / spacing_ms {
            timer.advance_ms(spacing_ms);
            store.reset();
            publish_one(&mut engine, &writer, &format!("dir-{commit}")).await;
            let operations = store.take();
            assert!(
                matches!(operations.as_slice(), [put] if is_wal_put(put, &namespace_id)),
                "{spacing_ms} ms apart, commit {commit}: {operations:?}"
            );
        }
    }
}

/// Checks that a quiet writer whose next WAL number a new writer session's
/// fence took collides with the fence, and that its retry runs discovery,
/// which finds the new epoch.
#[tokio::test]
async fn a_quiet_writer_collides_with_a_takeover_fence_and_is_fenced() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer_epoch(&context("writer-b").writer_id)
        .await
        .expect("writer b takes over");

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    let deadline = OperationClock::start(writer.now_ms, Arc::clone(&engine.namespace.timer));
    let collided = engine
        .publish_batch(vec![create_dir("beta", "beta")], &deadline)
        .await;
    assert_matches!(
        collided.results.as_slice(),
        [Err(CoreError::WalPublish(
            crate::wal::WalPublishError::NumberTaken
        ))]
    );
    let operations = store.take();
    assert!(
        matches!(operations.as_slice(), [put] if is_wal_put(put, &namespace_id)),
        "{operations:?}"
    );
    let retried = engine
        .publish_batch(vec![create_dir("beta", "beta")], &deadline)
        .await;
    assert_eq!(
        retried.results[0]
            .as_ref()
            .expect_err("displaced writer")
            .code(),
        ErrorCode::WriterFenced
    );
    assert_eq!(
        store.count(OperationSelector::Put),
        0,
        "the retry puts nothing"
    );
}

/// Checks that a writer whose next WAL number another writer takes while
/// the writer's put is in flight is stopped by its own put. The put
/// collides, the writer drops its position, and its retry finds that it was
/// fenced.
#[tokio::test]
async fn a_takeover_after_the_last_check_is_found_by_the_put() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = RecordingStore::new(
        BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(wal_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
        ),
        KeyPredicate::any(),
    );
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    store.inner().block_next();
    let deadline = OperationClock::start(writer.now_ms, Arc::clone(&engine.namespace.timer));
    let (published, rival) = futures::join!(
        engine.publish_batch(vec![create_dir("beta", "beta")], &deadline),
        async {
            store.inner().wait_until_blocked().await;
            let rival = crate::test_support::namespace_store(store.inner().inner(), &namespace_id)
                .acquire_writer_epoch(&context("writer-b").writer_id)
                .await
                .expect("writer b takes over");
            store.inner().release();
            rival
        }
    );
    assert_matches!(
        published.results.as_slice(),
        [Err(CoreError::WalPublish(
            crate::wal::WalPublishError::NumberTaken
        ))]
    );
    assert!(engine.read_position().is_none());
    let operations = store.take();
    assert!(
        matches!(operations.as_slice(), [put] if is_wal_put(put, &namespace_id)),
        "{operations:?}"
    );

    let retried = engine
        .publish_batch(vec![create_dir("beta", "beta")], &deadline)
        .await;
    let [Err(CoreError::WriterFenced(fence))] = retried.results.as_slice() else {
        panic!("displaced writer: {:?}", retried.results);
    };
    assert_eq!(fence.active_epoch, rival.writer_epoch);
    assert_eq!(fence.fenced_epoch, WriterEpoch(rival.writer_epoch.0 - 1));
    assert_eq!(
        fence.active_writer.as_ref().map(|writer| &writer.writer_id),
        Some(&rival.writer_id),
    );
    let boundary = loonfs_objectstore::keys::boundary(&namespace_id);
    let manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id);
    let operations = store.take();
    assert!(
        matches!(
            operations.as_slice(),
            [
                RecordedOperation::Get { key: first, .. }
                    | RecordedOperation::GetWithMetadata { key: first, .. },
                RecordedOperation::List { prefix },
                RecordedOperation::Get { key: manifest, .. }
                    | RecordedOperation::GetWithMetadata { key: manifest, .. },
                RecordedOperation::Get { key: last, .. }
                    | RecordedOperation::GetWithMetadata { key: last, .. },
            ] if first == &boundary
                && prefix == &manifests
                && manifest.starts_with(&manifests)
                && last == &boundary
        ),
        "{operations:?}"
    );
}

/// Checks that a quiet writer commits with its put alone over another
/// process's fold of its tail, and that a reader of the newest manifest
/// replays the commit.
#[tokio::test]
async fn a_quiet_writer_commits_above_another_process_fold() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal_tail(
            None,
            &OperationClock::start(0, Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .expect("another process folds");

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    publish_one(&mut engine, &writer, "beta").await;
    let operations = store.take();
    assert!(
        matches!(operations.as_slice(), [put] if is_wal_put(put, &namespace_id)),
        "{operations:?}"
    );
    let loaded = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    assert_eq!(
        engine
            .read_position()
            .expect("published")
            .read_basis()
            .head
            .seq,
        loaded.seq,
        "a reader replays the commit"
    );
}

/// Checks the first writer rule: a session with tip `t` whose manifest
/// probe finds another process's fold of a floor object at `t + 1` drops its
/// position instead of moving onto the fold, and its next commit is written
/// above `t + 1`, against the floor that the fold recorded.
#[tokio::test]
async fn a_session_drops_its_position_at_a_fold_past_its_tip() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, _timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let before = engine.writer_position().expect("a position");
    let tip = before.read_basis().head.wal_no;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let planned = namespace.load_current_manifest().await.expect("planned");
    namespace
        .publish_floor_object(&planned, CommitSeq(1))
        .await
        .expect("another process creates a floor object");
    let folded = namespace
        .fold_wal_tail(
            None,
            &OperationClock::start(0, Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .expect("another process folds the floor object")
        .current;
    assert_eq!(
        folded.manifest().folded_wal_no(),
        tip.successor().expect("next WAL number")
    );

    let newest = namespace
        .same_epoch_manifests_above(before.basis(), crate::test_support::observed_now())
        .await
        .expect("the probe's walk")
        .expect("the fold has the session's epoch");
    engine.record_manifest_published(newest);
    assert!(
        engine.writer_position().is_none(),
        "the session drops its position"
    );
    publish_one(&mut engine, &writer, "beta").await;
    let position = engine.writer_position().expect("a position");
    assert_eq!(position.read_basis().head.wal_no.0, tip.0 + 2);
    assert_eq!(position.retention_floor_seq(), CommitSeq(1));
}

/// Checks that a writer at the write stop whose fold finds its tail already
/// folded by another process moves its position onto that process's
/// manifest, so the writer's next commit succeeds.
#[tokio::test]
async fn a_writer_at_the_write_stop_moves_onto_another_process_fold_of_its_tail() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let mut index = 0;
    while engine.wal_tail_objects() < Some(crate::limits::MAX_UNFOLDED_WAL_OBJECTS) {
        index += 1;
        publish_one(&mut engine, &writer, &format!("dir-{index}")).await;
    }
    let refused = engine
        .publish_batch(
            vec![create_dir("refused", "refused")],
            &OperationClock::start(writer.now_ms, timer.clone()),
        )
        .await;
    assert_matches!(
        refused.results.as_slice(),
        [Err(error)] if error.code() == ErrorCode::MaintenanceRequired
    );
    let deadline = OperationClock::start(0, timer.clone());
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let fold = |input| namespace.fold_wal_tail(input, &deadline);
    let input = engine.writer_position().expect("tail");
    let other = fold(None).await.expect("another process folds");
    assert_eq!(other.response.outcome, crate::FoldWalOutcome::Published);
    let folded = fold(Some(input)).await.expect("the session's fold");
    assert_eq!(
        folded.response.outcome,
        crate::FoldWalOutcome::AlreadyCurrent
    );
    engine.record_manifest_published(folded.current);
    assert_eq!(
        engine
            .read_position()
            .expect("the position moved")
            .read_basis()
            .basis
            .manifest(),
        &other.current.manifest().manifest()
    );
    assert_eq!(engine.wal_tail_objects(), Some(0));
    publish_one(&mut engine, &writer, "after").await;
}

/// Creates a writer that folds three times and moves its position after
/// each fold, and then runs a compaction unit that publishes the next
/// manifest.
async fn writer_after_own_compaction<'a>(
    store: &'a GatedStore,
    namespace_id: &NamespaceId,
) -> (
    NamespaceCommitEngine<&'a GatedStore>,
    Arc<loonfs_test_support::clock::ManualClock>,
    WriterAt,
    crate::namespace::Held<LoadedManifest>,
) {
    use crate::compaction::CompactionUnitOutcome;

    let (mut engine, timer, writer) = writer_after_one_publish(store, namespace_id).await;
    // Move the position after each fold, as the publisher does.
    for name in ["beta", "gamma", "delta"] {
        publish_one(&mut engine, &writer, name).await;
        let input = engine.writer_position().expect("retained tail");
        let folded = crate::test_support::namespace_store(store, namespace_id)
            .fold_wal_tail(Some(input), &OperationClock::start(0, timer.clone()))
            .await
            .expect("own fold");
        engine.record_manifest_published(folded.current);
    }
    let before = engine.writer_position().expect("re-anchored tail");
    let compacted = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            store,
            namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.segment_cache = Arc::clone(&engine.namespace.segment_cache);
        fixture.namespace.run_policy = crate::test_support::one_delta_run_policy();
        fixture
    }
    .engine()
    .compact_from_current()
    .await
    .expect("own compaction");
    let manifest = match compacted {
        CompactionUnitOutcome::Published { manifest, .. } => Some(manifest),
        _ => None,
    }
    .expect("compaction publishes a successor");
    assert_eq!(
        manifest.manifest().manifest().manifest_no,
        before
            .read_basis()
            .basis
            .manifest_no()
            .successor()
            .expect("successor")
    );
    let after = crate::test_support::namespace_store(store, namespace_id)
        .load_read_state()
        .await
        .expect("head");
    assert_eq!(
        after.wal_no,
        before.read_basis().head.wal_no,
        "compaction leaves the tip unchanged"
    );
    assert_eq!(after.writer_epoch, before.read_basis().head.writer_epoch);
    (engine, timer, writer, manifest)
}

/// Checks that a quiet writer whose own compaction published a manifest that
/// the position didn't move onto commits on its cached tail, with no check
/// before its put and no read of its WAL tail.
#[tokio::test]
async fn a_quiet_writer_commits_on_its_cached_tail_after_its_own_compaction() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer, _manifest) =
        writer_after_own_compaction(&store, &namespace_id).await;

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    let published = publish_one(&mut engine, &writer, "epsilon").await;
    assert_eq!(
        published.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
    let operations = store.take();
    let (put, before) = operations.split_last().expect("the put");
    assert!(
        is_wal_put(put, &namespace_id)
            && before.iter().all(|operation| {
                !matches!(
                    operation,
                    RecordedOperation::List { .. } | RecordedOperation::Head { .. }
                ) && !operation.key().starts_with(&wal_prefix(&namespace_id))
            }),
        "{operations:?}"
    );
}

/// Checks that a compaction unit publishes a manifest with the same
/// `NamespaceReadState` as the manifest it replaces, and that both manifests
/// list runs.
///
/// `same_head` depends on both facts to keep the position's WAL tail after a
/// compaction.
#[tokio::test]
async fn a_compaction_copies_the_read_state_of_the_manifest_it_replaces() {
    use crate::manifest::load_namespace_manifest_envelope;

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (engine, _timer, _writer, compacted) =
        writer_after_own_compaction(&store, &namespace_id).await;
    let replaced = engine
        .writer_position()
        .expect("position")
        .read_basis()
        .basis
        .clone();

    let replaced = load_namespace_manifest_envelope(&store, &namespace_id, &replaced.manifest_no())
        .await
        .expect("replaced manifest");
    let compacted = load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &compacted.manifest().manifest().manifest_no,
    )
    .await
    .expect("compacted manifest");
    assert_eq!(
        NamespaceReadState::from(compacted.payload()),
        NamespaceReadState::from(replaced.payload())
    );
    assert!(!replaced.payload().runs.is_empty());
    assert!(!compacted.payload().runs.is_empty());
}

/// Checks that a writer moved to its own compaction's manifest publishes on
/// that manifest without reading the WAL tail or the manifest boundary
/// again.
#[tokio::test]
async fn a_writer_moved_after_its_own_compaction_keeps_its_tail() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, _timer, writer, manifest) =
        writer_after_own_compaction(&store, &namespace_id).await;

    engine.record_manifest_published(manifest.clone());
    store.reset();
    let published = publish_one(&mut engine, &writer, "epsilon").await;
    assert_eq!(
        published.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
    assert_eq!(
        engine
            .read_position()
            .expect("put succeeded")
            .read_basis()
            .basis,
        MetadataBasis::new(manifest.manifest().manifest())
    );
    let operations = store.take();
    assert!(
        operations.iter().all(|operation| {
            operation.key() != loonfs_objectstore::keys::boundary(&namespace_id)
                && !(operation.key().starts_with(&wal_prefix(&namespace_id))
                    && matches!(operation, RecordedOperation::Get { .. }))
        }),
        "the publish reads neither the WAL tail nor the manifest boundary: {operations:?}"
    );
}

/// Checks that a writer moved to a fold that missed its newest commit keeps
/// that commit as its tail, and publishes on the fold's manifest without
/// reading the WAL tail or the manifest boundary again.
#[tokio::test]
async fn a_writer_moved_to_a_fold_that_missed_its_newest_commit_keeps_that_commit() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    publish_one(&mut engine, &writer, "beta").await;
    // A maintenance call in the writer's runtime, which shares the writer's
    // segment cache, folds the tail through beta. The writer then commits
    // gamma on the manifest that the fold replaced.
    let folded = crate::NamespaceStore {
        segment_cache: Arc::clone(&engine.namespace.segment_cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .fold_wal_tail(None, &OperationClock::start(0, timer.clone()))
    .await
    .expect("fold");
    let published = folded.published().expect("the fold publishes a manifest");
    publish_one(&mut engine, &writer, "gamma").await;

    engine.record_manifest_published(published.clone());
    let moved = engine.writer_position().expect("the position moved");
    assert_eq!(
        moved.read_basis().basis.manifest(),
        &published.manifest().manifest()
    );
    assert_eq!(moved.wal_tail_objects(), 1);
    assert_eq!(moved.tail.chunks().len(), 1);
    let receipt = |commit_id| {
        engine.writer_position().is_some_and(|position| {
            position
                .tail
                .find_commit_receipt(&CommitId::parse(commit_id).expect("id"))
                .is_some()
        })
    };
    assert!(receipt("gamma") && !receipt("beta"));
    store.reset();
    let delta = publish_one(&mut engine, &writer, "delta").await;
    assert_eq!(
        delta.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
    let written = engine.read_position().expect("put succeeded");
    assert_eq!(
        written.read_basis().basis.manifest(),
        &published.manifest().manifest()
    );
    let operations = store.take();
    assert!(
        operations.iter().all(|operation| {
            operation.key() != loonfs_objectstore::keys::boundary(&namespace_id)
                && !(operation.key().starts_with(&wal_prefix(&namespace_id))
                    && matches!(operation, RecordedOperation::Get { .. }))
        }),
        "the publish reads neither the WAL tail nor the manifest boundary: {operations:?}"
    );
    let loaded = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    assert_eq!(
        written.read_basis().head,
        loaded,
        "a reader loads the head the writer has"
    );
}

/// Checks that a manifest this runtime published two numbers above the
/// position's manifest, over another process's fold, moves the position with
/// the commit after that fold as its tail, and that the start of the
/// publication becomes the position's last successor check.
#[tokio::test]
async fn a_manifest_published_over_another_process_fold_moves_the_position() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    publish_one(&mut engine, &writer, "beta").await;
    let basis = engine
        .writer_position()
        .expect("position")
        .read_basis()
        .basis
        .clone();
    // Another process folds the tail through beta, and the writer then
    // commits gamma on the manifest that the fold replaced.
    let folded = crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal_tail(None, &OperationClock::start(0, timer.clone()))
        .await
        .expect("fold");
    assert_eq!(folded.response.outcome, crate::FoldWalOutcome::Published);
    publish_one(&mut engine, &writer, "gamma").await;
    timer.advance_ms(10);
    let changed = crate::test_support::change_retention_window(
        &{
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.timer = timer.clone();
            fixture
        }
        .engine(),
    )
    .await;
    assert_eq!(
        changed.manifest().manifest().manifest_no.0,
        basis.manifest_no().0 + 2
    );

    engine.record_manifest_published(changed.clone());
    let moved = engine.writer_position().expect("the position moved");
    assert_eq!(
        moved.read_basis().basis.manifest(),
        &changed.manifest().manifest()
    );
    assert_eq!(
        moved.read_basis().head.folded_wal_no,
        folded.current.manifest().payload().folded_wal_no
    );
    assert_eq!(moved.wal_tail_objects(), 1);
    let receipt = |commit_id| {
        engine.writer_position().is_some_and(|position| {
            position
                .tail
                .find_commit_receipt(&CommitId::parse(commit_id).expect("id"))
                .is_some()
        })
    };
    assert!(receipt("gamma") && !receipt("beta"));
    assert_eq!(
        engine
            .writer_position()
            .map(|position| position.basis().checked().clone()),
        Some(changed.checked().clone())
    );
}

/// Checks that a manifest at the head of a manifest that lists no runs keeps
/// the position's whole tail, including the root directory that replay adds
/// above such a manifest, so the next commit still finds the root.
#[tokio::test]
async fn a_window_change_over_a_manifest_without_runs_keeps_the_root_directory() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let before = engine.writer_position().expect("position");
    let changed = crate::test_support::change_retention_window(
        &{
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.timer = timer.clone();
            fixture
        }
        .engine(),
    )
    .await;

    engine.record_manifest_published(changed.clone());
    let moved = engine.writer_position().expect("the position moved");
    assert_eq!(
        moved.read_basis().basis.manifest(),
        &changed.manifest().manifest()
    );
    assert!(Arc::ptr_eq(&moved.tail, &before.tail));
    let published = publish_one(&mut engine, &writer, "beta").await;
    assert_eq!(
        published.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
}

/// Checks that a retry of a committed request after a quiet hour replays
/// the request's receipt from the position and sends no request.
#[tokio::test]
async fn a_receipt_replay_after_a_quiet_hour_sends_no_request() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    publish_one(&mut engine, &writer, "alpha").await;
    let operations = store.take();
    assert!(operations.is_empty(), "{operations:?}");
}

#[tokio::test]
async fn publish_views_reuse_cached_segment_blocks_across_publishes() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let mut seed = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    seed.publish_batch(vec![create_dir("seed-commit", "docs")], &writer.clock())
        .await
        .results
        .remove(0)
        .expect("seed publish");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    namespace
        .create_snapshot_record(
            loonfs_types::format::control::SnapshotOwner::User {
                name: Some("test-snapshot".to_owned()),
                expires_at_ms: None,
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &writer.clock(),
        )
        .await
        .map(|(record, _)| crate::snapshot::snapshot_record_summary(record))
        .expect("snapshot");

    // Without a cache, every publish view fetches again the segment blocks
    // that resolving `/docs` reads.
    let mut uncached = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    store.reset();
    uncached
        .publish_batch(
            vec![create_dir("uncached-a", "docs/alpha")],
            &writer.clock(),
        )
        .await
        .results
        .remove(0)
        .expect("uncached publish a");
    assert!(
        store.count(OperationSelector::Read) > 0,
        "planning a create under `/docs` should read segment blocks"
    );
    store.reset();
    uncached
        .publish_batch(vec![create_dir("uncached-b", "docs/beta")], &writer.clock())
        .await
        .results
        .remove(0)
        .expect("uncached publish b");
    assert!(
        store.count(OperationSelector::Read) > 0,
        "without a cache the next publish re-fetches the same blocks"
    );

    let cache = Arc::new(MetadataSegmentCache::unshared(usize::MAX));
    let mut cached = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            segment_cache: cache,
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        writer.writer_id.clone(),
    );
    store.reset();
    cached
        .publish_batch(vec![create_dir("cached-a", "docs/gamma")], &writer.clock())
        .await
        .results
        .remove(0)
        .expect("cached publish a");
    assert!(
        store.count(OperationSelector::Read) > 0,
        "the first cached publish fills the cache"
    );
    store.reset();
    cached
        .publish_batch(vec![create_dir("cached-b", "docs/delta")], &writer.clock())
        .await
        .results
        .remove(0)
        .expect("cached publish b");
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "a warm cache serves every publish-view segment read"
    );
}

/// Advances the retention floor to `target` the way a writer session does:
/// creates a floor object at the position's tip as one unit of the session,
/// folds the tail from the position, and moves the position onto the fold's
/// manifest, which records the floor. Returns that manifest.
pub(super) async fn advance_in_session<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine<S>,
    target: CommitSeq,
) -> crate::namespace::Held<LoadedManifest> {
    let clock = OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
    let planned = engine
        .namespace
        .load_current_manifest()
        .await
        .expect("load the planned manifest");
    let report = engine
        .publish_floor_object(&planned, target, &clock)
        .await
        .expect("create the floor object");
    assert_eq!(report.retention_floor_seq, target, "the floor moves");
    let folded = engine
        .namespace
        .fold_wal_tail(engine.writer_position(), &clock)
        .await
        .expect("fold")
        .current;
    engine.record_manifest_published(folded.clone());
    folded
}
