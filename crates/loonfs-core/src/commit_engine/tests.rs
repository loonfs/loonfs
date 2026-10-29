//! Commit engine tests: admission weight, identity, ceilings, fencing, basis
//! checks, publish budgets, and publish-view caching.

mod content;
mod deletion;
mod inline;
mod retention_floor;

use super::*;
use crate::error::ErrorCode;
use crate::limits::{READ_REVALIDATION_BOUND_MS, WAL_PUBLISH_BUDGET_MS};
use crate::manifest::fold_wal_tail;
use crate::namespace::control::load_namespace_read_state;
use crate::test_support::ops::create;
use futures::StreamExt;
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{
    ChangeSeq, ContentRef, PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
    WriterEpoch,
};
use std::assert_matches;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::tempdir;

fn context(writer_id: &str) -> MutationContext {
    crate::test_support::mutation_context(writer_id, 1_000)
}

fn create_dir_request(commit_id: &str, name: &str) -> CommitIntent {
    CommitIntent::single(
        CommitId::parse(commit_id).expect("valid commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: loonfs_types::AbsolutePath::parse(format!("/{name}")).expect("valid path"),
            parents: false,
        },
    )
}

#[test]
fn admission_weight_includes_annotation_proofs_and_preparation_errors() {
    let request = create_dir_request("weight", "docs");
    let baseline = CommitCandidate::new(request.clone())
        .estimated_retained_bytes()
        .expect("weight");
    let mut annotated = request.clone();
    annotated.message = Some(String::new());
    let empty_annotation = CommitCandidate::new(annotated.clone())
        .estimated_retained_bytes()
        .expect("weight");
    annotated.message = Some("x".repeat(4096));
    assert!(
        CommitCandidate::new(annotated)
            .estimated_retained_bytes()
            .expect("weight")
            >= empty_annotation + 4096
    );
    let proof = PreparedContent::for_durable_content_write(ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        b"proof",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    ));
    let prepared = CommitCandidate::prepared(request.clone(), vec![proof; 100]);
    assert!(
        prepared.estimated_retained_bytes().expect("weight")
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
    assert!(rejected.estimated_retained_bytes().expect("weight") > baseline + 40960);
}

#[test]
fn principals_add_to_candidate_admission_weight() {
    let candidate = CommitCandidate::new(create_dir_request("subject-weight", "docs"));
    let before = candidate.estimated_retained_bytes().expect("weight");
    let principals = (0..64)
        .map(|index| {
            PrincipalId::parse(format!("{index:03}{}", "x".repeat(253))).expect("principal")
        })
        .collect::<BTreeSet<_>>();
    let candidate = candidate.with_subject(Subject {
        principal_scope: PrincipalScope::parse("scope").expect("scope"),
        subject_id: SubjectId::parse("subject").expect("subject"),
        principals: PrincipalSet::new(principals).expect("principals"),
    });
    let after = candidate.estimated_retained_bytes().expect("weight");
    assert!(after >= before + 64 * 256);
}

#[test]
fn semantic_identity_excludes_content_preparation() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let request = create_dir_request("same-mutation", "docs");
    let ready = CommitCandidate::new(request.clone());
    let rejected = CommitCandidate::rejected(
        request,
        ContentPreparationError::ContentToken(vec![(
            ContentId::generate(),
            ContentTokenError::Expired,
        )]),
    );

    assert_eq!(
        ready.semantic_identity(&namespace_id).expect("identity"),
        rejected.semantic_identity(&namespace_id).expect("identity")
    );
}

#[test]
fn semantic_identity_ignores_current_request_limits() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let oversized_ops = CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("too-many-ops").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations: (0..=crate::limits::MAX_COMMIT_OPERATIONS)
            .map(|index| FilesystemOperation::CreateDirectory {
                path: loonfs_types::AbsolutePath::parse(format!("/dir-{index}"))
                    .expect("valid path"),
                parents: false,
            })
            .collect(),
    });
    oversized_ops
        .semantic_identity(&namespace_id)
        .expect("operation limits must not affect identity");

    let content_ref = ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        b"proof",
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let prepared = PreparedContent::for_durable_content_write(content_ref);
    let oversized_proofs = CommitCandidate::prepared(
        create_dir_request("too-many-proofs", "docs"),
        vec![prepared; crate::limits::MAX_COMMIT_CONTENT_TOKENS + 1],
    );
    oversized_proofs
        .semantic_identity(&namespace_id)
        .expect("prepared proof limits must not affect identity");

    let oversized_message = CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("too-long-message").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES + 1)),
        operations: vec![FilesystemOperation::CreateDirectory {
            path: loonfs_types::AbsolutePath::parse("/docs").expect("valid path"),
            parents: false,
        }],
    });
    oversized_message
        .semantic_identity(&namespace_id)
        .expect("message limits must not affect identity");
}

#[test]
fn a_batch_past_the_operation_ceiling_is_rejected() {
    let oversized = CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("oversized-batch").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations: (0..=crate::limits::MAX_COMMIT_OPERATIONS)
            .map(|index| FilesystemOperation::CreateDirectory {
                path: loonfs_types::AbsolutePath::parse(format!("/dir-{index}"))
                    .expect("valid path"),
                parents: false,
            })
            .collect(),
    });

    let error = oversized
        .validate_request_limits()
        .expect_err("the batch is over the operation ceiling");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let at_ceiling = CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("largest-batch").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations: (0..crate::limits::MAX_COMMIT_OPERATIONS)
            .map(|index| FilesystemOperation::CreateDirectory {
                path: loonfs_types::AbsolutePath::parse(format!("/dir-{index}"))
                    .expect("valid path"),
                parents: false,
            })
            .collect(),
    });
    at_ceiling
        .validate_request_limits()
        .expect("a batch at the ceiling is admitted");
}

#[test]
fn a_message_past_the_byte_ceiling_is_rejected() {
    let operations = vec![FilesystemOperation::CreateDirectory {
        path: loonfs_types::AbsolutePath::parse("/docs").expect("valid path"),
        parents: false,
    }];

    let oversized = CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("oversized-message").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES + 1)),
        operations: operations.clone(),
    });
    let error = oversized
        .validate_request_limits()
        .expect_err("the message is over the byte ceiling");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let at_ceiling = CommitCandidate::new(CommitIntent {
        preconditions: Vec::new(),
        commit_id: CommitId::parse("largest-message").expect("valid commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: Some("m".repeat(crate::limits::MAX_COMMIT_MESSAGE_BYTES)),
        operations,
    });
    at_ceiling
        .validate_request_limits()
        .expect("a message at the ceiling is admitted");
}

fn create_dir(commit_id: &str, display_name: &str) -> CommitCandidate {
    CommitCandidate::new(create_dir_request(commit_id, display_name))
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

    create(store.inner(), &namespace_id, &writer_a)
        .await
        .expect("bootstrap");

    let mut engine_a = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    engine_a
        .publish_batch(
            store.inner(),
            vec![create_dir("from-a-first", "alpha")],
            &writer_a,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results[0]
        .as_ref()
        .expect("writer a first commit");
    // Force a fresh manifest load on the next publish.
    engine_a.invalidate_projection();

    store.block_next();
    let blocked_store = StdArc::clone(&store);
    let publish_a = tokio::spawn(async move {
        let mut engine = engine_a;
        let result = engine
            .publish_batch(
                blocked_store.as_ref(),
                vec![create_dir("from-a-second", "gamma")],
                &writer_a,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
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
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    engine_b
        .publish_batch(
            store.inner(),
            vec![create_dir("from-b-first", "beta")],
            &writer_b,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results[0]
        .as_ref()
        .expect("writer b takeover commit");
    store.release();

    let code = publish_a.await.expect("join publish a");
    assert_eq!(code, Some(ErrorCode::WriterFenced));
}

#[tokio::test]
async fn shared_session_keeps_fencing_across_engine_rebuilds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer_a = context("writer-a");
    create(&store, &namespace_id, &writer_a)
        .await
        .expect("bootstrap");

    let session = SharedWriterSessionStatus::default();
    let mut engine_a1 = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .writer_session(Arc::clone(&session));
    engine_a1
        .publish_batch(
            &store,
            vec![create_dir("from-a-first", "alpha")],
            &writer_a,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .remove(0)
        .expect("writer a first commit");

    let writer_b = context("writer-b");
    let mut engine_b = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    engine_b
        .publish_batch(
            &store,
            vec![create_dir("from-b-first", "beta")],
            &writer_b,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .remove(0)
        .expect("writer b takeover commit");

    for expected in [ErrorCode::StaleHead, ErrorCode::WriterFenced] {
        let result = engine_a1
            .publish_batch(
                &store,
                vec![create_dir("from-a-second", "gamma")],
                &writer_a,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
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
    let epoch_after_fencing =
        load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
            .await
            .expect("read head")
            .writer_epoch;

    // A rebuilt engine — cache eviction, cache-disabled mode — shares
    // the session state, so the session stays terminally fenced and
    // never touches the head.
    drop(engine_a1);
    let mut engine_a2 = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .writer_session(session);
    let still_fenced = engine_a2
        .publish_batch(
            &store,
            vec![create_dir("from-a-third", "delta")],
            &writer_a,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await;
    let error = still_fenced.results[0]
        .as_ref()
        .expect_err("rebuilt engine stays fenced");
    assert_eq!(error.code(), ErrorCode::WriterFenced);
    let head = load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
        .await
        .expect("read head");
    assert_eq!(head.writer_epoch, epoch_after_fencing);
    assert_eq!(
        head.writer.expect("writer block").writer_id.as_str(),
        "writer-b"
    );
}

/// Advances an entire publish budget per reading, so every publish
/// observes an expired budget before its WAL object PUT.
#[derive(Debug)]
struct ExpiredBudgetTimer(AtomicU64);

impl MonotonicTimer for ExpiredBudgetTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0
            .fetch_add(WAL_PUBLISH_BUDGET_MS + 1_000, Ordering::SeqCst)
    }
}

#[tokio::test]
async fn a_landed_put_refreshes_the_observation_the_next_budget_runs_from() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_test_support::stores::KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = context("writer-a");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .monotonic_timer(timer.clone());
    for (name, advance_ms) in [
        ("alpha", 0),
        ("beta", WAL_PUBLISH_BUDGET_MS - 1),
        ("gamma", 2),
    ] {
        timer.advance_ms(advance_ms);
        store.reset();
        let published = engine
            .publish_batch(
                &store,
                vec![create_dir(name, name)],
                &writer,
                &Deadline::start(Arc::clone(&engine.timer)),
            )
            .await;
        published.results[0].as_ref().expect(name);
        // The tip observation is the start of the attempt whose put landed.
        // The HEAD that confirms an expired tip is the basis check below, so
        // the request count alone can't show which observation the budget
        // runs from.
        assert_eq!(
            engine.projection_observed.as_ref().expect("tip").age_ms(),
            0,
            "{name}"
        );
    }
    // More than the budget has passed since the first observation, but only
    // 2 ms since the last landed put, so the third publish rediscovers
    // nothing. Its basis has gone more than a budget unconfirmed, so before
    // its put it makes one HEAD of the next manifest number and nothing else.
    let operations = store.take();
    assert!(
        !operations
            .iter()
            .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)),
        "the third publish rediscovered the namespace: {operations:?}"
    );
    let heads: Vec<_> = operations
        .iter()
        .filter(|operation| matches!(operation, RecordedOperation::Head { .. }))
        .collect();
    assert_matches!(
        heads.as_slice(),
        [RecordedOperation::Head { key }] if key.starts_with(
            &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
        ),
        "{operations:?}"
    );
}

async fn publish_one<S: ObjectStore + ?Sized>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    writer: &MutationContext,
    name: &str,
) -> NamespaceCommitEnginePublishResult {
    let deadline = Deadline::start(Arc::clone(&engine.timer));
    let published = engine
        .publish_batch(store, vec![create_dir(name, name)], writer, &deadline)
        .await;
    published.results[0].as_ref().expect(name);
    published
}

#[tokio::test]
async fn a_fold_by_another_process_is_found_at_the_basis_check() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = context("writer-a");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .monotonic_timer(timer.clone());
    publish_one(&mut engine, &store, &writer, "alpha").await;
    let basis = engine.read_position().expect("landed").basis.manifest_no();

    // Another process folds the tail into the next manifest.
    fold_wal_tail(
        &store,
        None,
        &namespace_id,
        None,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &crate::test_support::content_writes(),
    )
    .await
    .expect("fold");

    // A budget after the load the basis and the tip are still trusted.
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS);
    store.reset();
    publish_one(&mut engine, &store, &writer, "beta").await;
    assert_eq!(
        engine.read_position().expect("landed").basis.manifest_no(),
        basis
    );
    assert_eq!(store.count(OperationSelector::Head), 0);

    // Two budgets after the load the check finds the successor, and the
    // view is reloaded from the hint before the put.
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS);
    store.reset();
    publish_one(&mut engine, &store, &writer, "gamma").await;
    let operations = store.take();
    assert_eq!(
        engine.read_position().expect("landed").basis.manifest_no(),
        basis.successor().expect("next manifest number")
    );
    let head = operations
        .iter()
        .position(|operation| matches!(operation, RecordedOperation::Head { .. }))
        .expect("basis check");
    assert!(
        operations[head..]
            .iter()
            .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)),
        "{operations:?}"
    );
}

#[tokio::test]
async fn a_basis_check_answered_outside_the_bound_of_the_last_confirmation_reloads_instead() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = RecordingStore::new(
        BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
                &namespace_id,
            )),
            OperationSelector::Head,
        ),
        KeyPredicate::any(),
    );
    let writer = context("writer-a");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .monotonic_timer(timer.clone());
    publish_one(&mut engine, &store, &writer, "alpha").await;
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS - 1);
    publish_one(&mut engine, &store, &writer, "beta").await;

    // The third publish owes a basis check. Its answer arrives inside the
    // revalidation bound of the attempt's start but outside the bound of
    // the confirmation at zero, so it cannot be trusted: the view is
    // reloaded, and the batch itself is then over its budget.
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS - 1);
    store.reset();
    store.inner().block_next();
    let deadline = Deadline::start(Arc::clone(&engine.timer));
    let (published, ()) = futures::join!(
        engine.publish_batch(
            &store,
            vec![create_dir("gamma", "gamma")],
            &writer,
            &deadline,
        ),
        async {
            store.inner().wait_until_blocked().await;
            timer.advance_ms(READ_REVALIDATION_BOUND_MS - 1);
            store.inner().release();
        }
    );
    let error = published.results[0]
        .as_ref()
        .expect_err("a batch older than its budget writes nothing");
    assert_matches!(
        error,
        CoreError::WalPublish(crate::wal::WalPublishError::PublishBudgetExceeded { .. }),
        "unexpected error: {error:?}"
    );
    let operations = store.take();
    let head = operations
        .iter()
        .position(|operation| matches!(operation, RecordedOperation::Head { .. }))
        .expect("basis check");
    assert!(
        operations[head + 1..]
            .iter()
            .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)),
        "the late check was trusted: {operations:?}"
    );
    // The retry plans against the reloaded view.
    publish_one(&mut engine, &store, &writer, "gamma").await;
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
/// engine, whose first publish loaded its view and landed.
async fn writer_after_one_publish(
    store: &GatedStore,
    namespace_id: &NamespaceId,
) -> (
    NamespaceCommitEngine,
    Arc<loonfs_test_support::clock::ManualClock>,
    MutationContext,
) {
    let writer = context("writer-a");
    create(store, namespace_id, &writer)
        .await
        .expect("bootstrap");
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .monotonic_timer(timer.clone())
    .segment_cache(Arc::new(MetadataSegmentCache::unshared(usize::MAX)));
    publish_one(&mut engine, store, &writer, "alpha").await;
    (engine, timer, writer)
}

fn is_head_under(operation: &RecordedOperation, prefix: &str) -> bool {
    matches!(operation, RecordedOperation::Head { key } if key.starts_with(prefix))
}

#[tokio::test]
async fn a_quiet_writer_confirms_its_tip_with_one_head() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    for name in ["beta", "gamma", "delta"] {
        publish_one(&mut engine, &store, &writer, name).await;
    }

    timer.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
    store.reset();
    publish_one(&mut engine, &store, &writer, "epsilon").await;
    // The tail holds a fence and four commits, and none of it is read
    // again: a HEAD of the next manifest number, then the put.
    let operations = store.take();
    assert_matches!(
        operations.as_slice(),
        [
            RecordedOperation::Head { key: successor },
            RecordedOperation::Put {
                key: written,
                mode: loonfs_objectstore::PutMode::CreateIfAbsent,
                ..
            },
        ] if successor.starts_with(
                &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
            )
            && written.starts_with(&wal_prefix(&namespace_id))
    );
}

/// Checks that a writer that commits every few minutes for an hour, against
/// a manifest that nothing replaces, sends one HEAD of the next manifest
/// number on each commit whose basis was confirmed more than a budget
/// earlier, and no other request before its put, so it never reads its WAL
/// tail again.
///
/// Commits a whole budget apart check the basis when it's two budgets old,
/// and the revalidation bound still trusts that check.
#[tokio::test]
async fn a_steady_writer_checks_its_basis_once_a_budget_and_never_replays_its_tail() {
    const HOUR_MS: u64 = 60 * 60 * 1000;
    for spacing_ms in [4 * 60 * 1000, WAL_PUBLISH_BUDGET_MS] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id);
        let store = gated_store(&directory, KeyPredicate::new(|_| false));
        let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
        let mut confirmed_at_ms = 0;
        let mut checks = 0;
        let mut tail_reads = 0;
        for commit in 1..=HOUR_MS / spacing_ms {
            timer.advance_ms(spacing_ms);
            let now_ms = commit * spacing_ms;
            let due = now_ms - confirmed_at_ms > WAL_PUBLISH_BUDGET_MS;
            store.reset();
            publish_one(&mut engine, &store, &writer, &format!("dir-{commit}")).await;
            let operations = store.take();
            tail_reads += operations
                .iter()
                .filter(|operation| {
                    operation.key() == loonfs_objectstore::keys::hint(&namespace_id)
                        || (operation.key().starts_with(&wal_prefix(&namespace_id))
                            && !matches!(operation, RecordedOperation::Put { .. }))
                })
                .count();
            let (put, probes) = operations.split_last().expect("the put");
            assert!(
                probes.len() == usize::from(due)
                    && probes
                        .iter()
                        .all(|operation| is_head_under(operation, &manifests))
                    && matches!(
                        put,
                        RecordedOperation::Put {
                            key,
                            mode: loonfs_objectstore::PutMode::CreateIfAbsent,
                            ..
                        } if key.starts_with(&wal_prefix(&namespace_id))
                    ),
                "{spacing_ms} ms apart, commit {commit}: {operations:?}"
            );
            if due {
                confirmed_at_ms = now_ms;
                checks += 1;
            }
        }
        assert_eq!(tail_reads, 0, "{spacing_ms} ms apart");
        // Each check confirms the basis for the commit after it, so every
        // other commit checks.
        assert_eq!(checks, HOUR_MS / spacing_ms / 2, "{spacing_ms} ms apart");
    }
}

#[tokio::test]
async fn a_quiet_writer_finds_a_takeover_claim_and_is_fenced() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    crate::namespace::writer_epoch::acquire_writer_epoch(
        &store,
        &namespace_id,
        &context("writer-b"),
    )
    .await
    .expect("writer b takes over");

    timer.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
    store.reset();
    let published = engine
        .publish_batch(
            &store,
            vec![create_dir("beta", "beta")],
            &writer,
            &Deadline::start(Arc::clone(&engine.timer)),
        )
        .await;
    assert_eq!(
        published.results[0]
            .as_ref()
            .expect_err("displaced writer")
            .code(),
        ErrorCode::WriterFenced
    );
    let operations = store.take();
    assert!(
        is_head_under(
            &operations[0],
            &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
        ),
        "{operations:?}"
    );
    assert!(
        operations
            .iter()
            .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)),
        "{operations:?}"
    );
    assert_eq!(store.count(OperationSelector::Put), 0, "{operations:?}");
}

/// Checks that a writer whose next WAL number another writer takes after
/// the writer's last check is stopped by its own put. The put collides, the
/// writer drops its position, and its retry finds that it was fenced.
///
/// Before the put, the writer sends no request within its publication
/// budget, and one HEAD of the next manifest number after it.
#[tokio::test]
async fn a_takeover_after_the_last_check_is_found_by_the_put() {
    for (quiet_ms, manifest_heads) in [(WAL_PUBLISH_BUDGET_MS, 0), (WAL_PUBLISH_BUDGET_MS + 1, 1)] {
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

        timer.advance_ms(quiet_ms);
        store.reset();
        store.inner().block_next();
        let deadline = Deadline::start(Arc::clone(&engine.timer));
        let (published, ()) = futures::join!(
            engine.publish_batch(&store, vec![create_dir("beta", "beta")], &writer, &deadline),
            async {
                store.inner().wait_until_blocked().await;
                crate::namespace::writer_epoch::acquire_writer_epoch(
                    store.inner().inner(),
                    &namespace_id,
                    &context("writer-b"),
                )
                .await
                .expect("writer b takes over");
                store.inner().release();
            }
        );
        assert_matches!(
            published.results.as_slice(),
            [Err(CoreError::WalPublish(
                crate::wal::WalPublishError::NumberTaken
            ))]
        );
        assert!(engine.read_position().is_none(), "{quiet_ms}");
        let operations = store.take();
        let (put, probes) = operations.split_last().expect("the put");
        assert!(
            probes.len() == manifest_heads
                && probes.iter().all(|operation| is_head_under(
                    operation,
                    &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
                ))
                && matches!(
                    put,
                    RecordedOperation::Put {
                        key,
                        mode: loonfs_objectstore::PutMode::CreateIfAbsent,
                        ..
                    } if key.starts_with(&wal_prefix(&namespace_id))
                ),
            "{quiet_ms}: {operations:?}"
        );

        let retried = engine
            .publish_batch(&store, vec![create_dir("beta", "beta")], &writer, &deadline)
            .await;
        assert_eq!(
            retried.results[0]
                .as_ref()
                .expect_err("displaced writer")
                .code(),
            ErrorCode::WriterFenced
        );
    }
}

#[tokio::test]
async fn a_quiet_writer_finds_a_fold_and_lands_on_the_new_basis() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let basis = engine
        .wal_fold_input()
        .expect("retained view")
        .basis
        .manifest_no();
    fold_wal_tail(
        &store,
        None,
        &namespace_id,
        None,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &crate::test_support::content_writes(),
    )
    .await
    .expect("another process folds");

    timer.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
    store.reset();
    publish_one(&mut engine, &store, &writer, "beta").await;
    assert_eq!(
        engine.read_position().expect("landed").basis.manifest_no(),
        basis.successor().expect("next manifest number")
    );
    let operations = store.take();
    assert!(
        is_head_under(
            &operations[0],
            &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
        ) && operations[1..]
            .iter()
            .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)),
        "{operations:?}"
    );
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
        publish_one(&mut engine, &store, &writer, &format!("dir-{index}")).await;
    }
    let refused = engine
        .publish_batch(
            &store,
            vec![create_dir("refused", "refused")],
            &writer,
            &Deadline::start(timer.clone()),
        )
        .await;
    assert_matches!(
        refused.results.as_slice(),
        [Err(error)] if error.code() == ErrorCode::MaintenanceRequired
    );
    let deadline = Deadline::start(timer.clone());
    let content_writes = crate::test_support::content_writes();
    let fold = |input| {
        fold_wal_tail(
            &store,
            None,
            &namespace_id,
            input,
            &deadline,
            crate::manifest::MetadataRunPolicy::default(),
            &content_writes,
        )
    };
    let input = engine.wal_fold_input().expect("tail");
    let other = fold(None).await.expect("another process folds");
    assert_eq!(other.response.outcome, crate::FoldWalOutcome::Published);
    let folded = fold(Some(input)).await.expect("the session's fold");
    assert_eq!(
        folded.response.outcome,
        crate::FoldWalOutcome::AlreadyCurrent
    );
    engine.record_manifest_published(folded.manifest);
    assert_eq!(
        engine.read_position().expect("the position moved").basis.0,
        other.manifest.manifest
    );
    assert_eq!(engine.wal_tail_objects(), Some(0));
    publish_one(&mut engine, &store, &writer, "after").await;
}

/// Creates a writer that folds three times and moves its position after
/// each fold, and then runs a compaction unit that publishes the next
/// manifest.
async fn writer_after_own_compaction(
    store: &GatedStore,
    namespace_id: &NamespaceId,
) -> (
    NamespaceCommitEngine,
    Arc<loonfs_test_support::clock::ManualClock>,
    MutationContext,
    PublishedManifest,
) {
    use crate::manifest::{
        compact_unit, CompactionUnitOutcome, MetadataCompactionPolicy, MetadataRunPolicy,
    };

    let (mut engine, timer, writer) = writer_after_one_publish(store, namespace_id).await;
    // Move the position after each fold, as the publisher does.
    for name in ["beta", "gamma", "delta"] {
        publish_one(&mut engine, store, &writer, name).await;
        let input = engine.wal_fold_input().expect("retained tail");
        let folded = fold_wal_tail(
            store,
            None,
            namespace_id,
            Some(input),
            &Deadline::start(timer.clone()),
            crate::manifest::MetadataRunPolicy::default(),
            &crate::test_support::content_writes(),
        )
        .await
        .expect("own fold");
        engine.record_manifest_published(folded.manifest);
    }
    let before = engine.wal_fold_input().expect("re-anchored tail");
    let current = crate::namespace::control::load_current_manifest(store, namespace_id)
        .await
        .expect("current manifest");
    let compacted = compact_unit(
        store,
        engine.segment_cache.as_deref(),
        namespace_id,
        current.state.compactor_epoch(),
        MetadataRunPolicy::default(),
        MetadataCompactionPolicy::CompactImmediately,
        crate::manifest::CompactionAdmission::AnySize,
        &crate::manifest::MetadataCompactionCancellation::default(),
        crate::test_support::std_timer(),
    )
    .await
    .expect("own compaction");
    let manifest = match compacted {
        CompactionUnitOutcome::Published { manifest, .. } => Some(manifest),
        _ => None,
    }
    .expect("compaction publishes a successor");
    assert_eq!(
        manifest.manifest.manifest_no,
        before.basis.manifest_no().successor().expect("successor")
    );
    let after = load_namespace_read_state(store, namespace_id, crate::test_support::std_timer())
        .await
        .expect("head");
    assert_eq!(
        after.wal_no, before.head.wal_no,
        "compaction leaves the tip unchanged"
    );
    assert_eq!(after.writer_epoch, before.head.writer_epoch);
    (engine, timer, writer, manifest)
}

#[tokio::test]
async fn a_quiet_writer_reloads_after_its_own_compaction() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer, manifest) =
        writer_after_own_compaction(&store, &namespace_id).await;

    timer.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
    store.reset();
    let published = publish_one(&mut engine, &store, &writer, "epsilon").await;
    assert_eq!(
        published.wal_tail.map(|tail| tail.source),
        Some(TailSource::Store)
    );
    assert_eq!(
        engine.read_position().expect("landed").basis.manifest_no(),
        manifest.manifest.manifest_no
    );
    let operations = store.take();
    assert!(is_head_under(
        &operations[0],
        &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id)
    ));
    assert!(operations[1..]
        .iter()
        .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)));
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
    let replaced = engine.wal_fold_input().expect("position").basis;

    let replaced = load_namespace_manifest_envelope(&store, &namespace_id, &replaced.manifest_no())
        .await
        .expect("replaced manifest");
    let compacted =
        load_namespace_manifest_envelope(&store, &namespace_id, &compacted.manifest.manifest_no)
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
/// that manifest without reading the WAL tail or the hint again.
#[tokio::test]
async fn a_writer_moved_after_its_own_compaction_keeps_its_tail() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, _timer, writer, manifest) =
        writer_after_own_compaction(&store, &namespace_id).await;

    engine.record_manifest_published(manifest.clone());
    store.reset();
    let published = publish_one(&mut engine, &store, &writer, "epsilon").await;
    assert_eq!(
        published.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
    assert_eq!(
        engine.read_position().expect("put succeeded").basis,
        MetadataBasis(manifest.manifest)
    );
    let operations = store.take();
    assert!(
        operations.iter().all(|operation| {
            operation.key() != loonfs_objectstore::keys::hint(&namespace_id)
                && !(operation.key().starts_with(&wal_prefix(&namespace_id))
                    && matches!(operation, RecordedOperation::Get { .. }))
        }),
        "the publish reads neither the WAL tail nor the hint: {operations:?}"
    );
}

/// Checks that a writer moved to a fold that missed its newest commit keeps
/// that commit as its tail, and publishes on the fold's manifest without
/// reading the WAL tail or the hint again.
#[tokio::test]
async fn a_writer_moved_to_a_fold_that_missed_its_newest_commit_keeps_that_commit() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    publish_one(&mut engine, &store, &writer, "beta").await;
    // A maintenance call folds the tail through beta. The writer then
    // commits gamma on the manifest that the fold replaced.
    let folded = fold_wal_tail(
        &store,
        None,
        &namespace_id,
        None,
        &Deadline::start(timer.clone()),
        crate::manifest::MetadataRunPolicy::default(),
        &crate::test_support::content_writes(),
    )
    .await
    .expect("fold");
    let published = folded.published().expect("the fold publishes a manifest");
    publish_one(&mut engine, &store, &writer, "gamma").await;

    engine.record_manifest_published(published.clone());
    let moved = engine.wal_fold_input().expect("the position moved");
    assert_eq!(moved.basis.manifest(), &published.manifest);
    assert_eq!(moved.wal_tail_objects, 1);
    assert_eq!(moved.tail_state.chunks().len(), 1);
    let receipt =
        |commit_id| engine.retains_commit_receipt(&CommitId::parse(commit_id).expect("id"));
    assert!(receipt("gamma") && !receipt("beta"));
    store.reset();
    let delta = publish_one(&mut engine, &store, &writer, "delta").await;
    assert_eq!(
        delta.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
    let written = engine.read_position().expect("put succeeded");
    assert_eq!(written.basis.manifest(), &published.manifest);
    let operations = store.take();
    assert!(
        operations.iter().all(|operation| {
            operation.key() != loonfs_objectstore::keys::hint(&namespace_id)
                && !(operation.key().starts_with(&wal_prefix(&namespace_id))
                    && matches!(operation, RecordedOperation::Get { .. }))
        }),
        "the publish reads neither the WAL tail nor the hint: {operations:?}"
    );
    let loaded = load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
        .await
        .expect("head");
    assert_eq!(
        written.head, loaded,
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
    publish_one(&mut engine, &store, &writer, "beta").await;
    let basis = engine.wal_fold_input().expect("position").basis;
    // Another process folds the tail through beta, and the writer then
    // commits gamma on the manifest that the fold replaced.
    let folded = fold_wal_tail(
        &store,
        None,
        &namespace_id,
        None,
        &Deadline::start(timer.clone()),
        crate::manifest::MetadataRunPolicy::default(),
        &crate::test_support::content_writes(),
    )
    .await
    .expect("fold");
    assert_eq!(folded.response.outcome, crate::FoldWalOutcome::Published);
    publish_one(&mut engine, &store, &writer, "gamma").await;
    timer.advance_ms(10);
    let claimed = crate::manifest::claim_compactor(&store, &namespace_id, timer.clone())
        .await
        .expect("claim the compactor")
        .published
        .expect("the claim publishes a manifest");
    assert_eq!(claimed.manifest.manifest_no.0, basis.manifest_no().0 + 2);

    engine.record_manifest_published(claimed.clone());
    let moved = engine.wal_fold_input().expect("the position moved");
    assert_eq!(moved.basis.0, claimed.manifest);
    assert_eq!(moved.head.folded_wal_no, folded.manifest.folded_wal_no);
    assert_eq!(moved.wal_tail_objects, 1);
    let receipt =
        |commit_id| engine.retains_commit_receipt(&CommitId::parse(commit_id).expect("id"));
    assert!(receipt("gamma") && !receipt("beta"));
    assert_eq!(engine.basis_checked, claimed.started);
}

/// Checks that a manifest at the head of a manifest that lists no runs keeps
/// the position's whole tail, including the root directory that replay adds
/// above such a manifest, so the next commit still finds the root.
#[tokio::test]
async fn a_claim_over_a_manifest_without_runs_keeps_the_root_directory() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let before = engine.wal_fold_input().expect("position");
    let claimed = crate::manifest::claim_compactor(&store, &namespace_id, timer.clone())
        .await
        .expect("claim the compactor")
        .published
        .expect("the claim publishes a manifest");

    engine.record_manifest_published(claimed.clone());
    let moved = engine.wal_fold_input().expect("the position moved");
    assert_eq!(moved.basis.0, claimed.manifest);
    assert!(Arc::ptr_eq(&moved.tail_state, &before.tail_state));
    let published = publish_one(&mut engine, &store, &writer, "beta").await;
    assert_eq!(
        published.wal_tail.map(|tail| tail.source),
        Some(TailSource::Cached)
    );
}

#[tokio::test]
async fn a_confirmed_batch_without_a_put_keeps_the_attempt_observation() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = gated_store(&directory, KeyPredicate::manifest(&namespace_id));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
    store.reset();
    store.inner().block_next();
    // Replaying the first receipt succeeds without a put. The delayed
    // answer must leave both clocks dated at the start of the attempt.
    let (replayed, ()) =
        futures::join!(publish_one(&mut engine, &store, &writer, "alpha"), async {
            store.inner().wait_until_blocked().await;
            timer.advance_ms(1_000);
            store.inner().release();
        });
    drop(replayed);
    assert_eq!(store.count(OperationSelector::Put), 0);
    assert_eq!(
        engine.projection_observed.as_ref().expect("tip").age_ms(),
        1_000
    );
    assert_eq!(
        engine.basis_checked.as_ref().expect("basis").age_ms(),
        1_000
    );
    timer.advance_ms(WAL_PUBLISH_BUDGET_MS - 1_000);
    store.reset();
    publish_one(&mut engine, &store, &writer, "beta").await;
    assert_matches!(
        store.take().as_slice(),
        [RecordedOperation::Put { .. }],
        "the exact budget boundary still trusts the confirmed tip"
    );
}

#[tokio::test]
async fn a_writer_quiet_for_the_revalidation_bound_rediscovers_without_probing() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;

    timer.advance_ms(READ_REVALIDATION_BOUND_MS);
    store.reset();
    publish_one(&mut engine, &store, &writer, "beta").await;
    let operations = store.take();
    assert_eq!(
        operations.first().map(RecordedOperation::key),
        Some(loonfs_objectstore::keys::hint(&namespace_id).as_str()),
        "{operations:?}"
    );
}

#[tokio::test]
async fn a_probe_answered_at_the_bound_after_the_last_confirmation_is_not_trusted() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::manifest(&namespace_id));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;

    // The attempt starts inside the bound of the last confirmation, and the
    // manifest HEAD answers at the bound: the view is reloaded, although
    // the answer came only a second after the attempt started.
    timer.advance_ms(READ_REVALIDATION_BOUND_MS - 1_000);
    store.reset();
    store.inner().block_next();
    let (_, ()) = futures::join!(publish_one(&mut engine, &store, &writer, "beta"), async {
        store.inner().wait_until_blocked().await;
        timer.advance_ms(1_000);
        store.inner().release();
    });
    assert!(engine.read_position().is_some());
    let operations = store.take();
    let successor = operations
        .iter()
        .position(|operation| {
            is_head_under(
                operation,
                &loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id),
            )
        })
        .expect("successor check");
    assert!(
        successor == 0
            && operations[successor + 1..]
                .iter()
                .any(|operation| operation.key() == loonfs_objectstore::keys::hint(&namespace_id)),
        "the late answer was trusted: {operations:?}"
    );
}

#[tokio::test]
async fn publish_over_budget_writes_no_wal_object_and_a_retry_rebuilds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = context("writer-a");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let mut over_budget = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    over_budget
        .session_writer_epoch(&store, &writer)
        .await
        .expect("acquire");
    let mut over_budget =
        over_budget.monotonic_timer(Arc::new(ExpiredBudgetTimer(AtomicU64::new(0))));
    let head_before =
        load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
            .await
            .expect("head");
    let abandoned = over_budget
        .publish_batch(
            &store,
            vec![create_dir("budgeted", "alpha")],
            &writer,
            &Deadline::start(Arc::clone(&over_budget.timer)),
        )
        .await;
    let error = abandoned.results[0]
        .as_ref()
        .expect_err("over-budget publish must abandon");
    assert_matches!(
        error,
        CoreError::WalPublish(crate::wal::WalPublishError::PublishBudgetExceeded { .. }),
        "unexpected error: {error:?}"
    );
    // Retryable exactly like a stale head, so existing retry loops
    // rebuild the commit.
    assert_eq!(error.code(), ErrorCode::StaleHead);

    let head_after =
        load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
            .await
            .expect("read head");
    assert_eq!(head_after.seq, head_before.seq);
    assert_eq!(head_after.wal_no, head_before.wal_no);
    assert_eq!(wal_object_count(&store, &namespace_id).await, 1);

    let mut healthy = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    let retried = healthy
        .publish_batch(
            &store,
            vec![create_dir("budgeted", "alpha")],
            &writer,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await;
    let response = retried.results[0].as_ref().expect("rebuilt publish");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    assert_eq!(wal_object_count(&store, &namespace_id).await, 3);
    let head_final =
        load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
            .await
            .expect("read head");
    assert_eq!(head_final.seq, ChangeSeq(1));
    // Two engines are two sessions, and each acquires its own epoch: the
    // abandoned attempt took 1, the retry took 2.
    assert_eq!(head_final.writer_epoch, WriterEpoch(2));
}

#[tokio::test]
async fn publish_views_reuse_cached_segment_blocks_across_publishes() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = context("writer-a");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let mut seed = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    seed.publish_batch(
        &store,
        vec![create_dir("seed-commit", "docs")],
        &writer,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .results
    .remove(0)
    .expect("seed publish");
    crate::pin::create_pin(
        &store,
        &namespace_id,
        loonfs_types::format::control::PinOwner::User {
            name: "test-pin".to_owned(),
            expires_at_ms: None,
        },
        &writer,
        Default::default(),
        None,
        &crate::test_support::content_writes(),
        crate::test_support::std_timer(),
    )
    .await
    .map(|report| crate::pin::checkpoint_summary(report.response))
    .expect("checkpoint");

    // Without a cache, every publish view re-fetches the segment blocks
    // its validation walks need.
    let mut uncached = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    store.reset();
    uncached
        .publish_batch(
            &store,
            vec![create_dir("uncached-a", "alpha")],
            &writer,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .remove(0)
        .expect("uncached publish a");
    assert!(
        store.count(OperationSelector::Read) > 0,
        "publish validation should read segment blocks"
    );
    store.reset();
    uncached
        .publish_batch(
            &store,
            vec![create_dir("uncached-b", "beta")],
            &writer,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
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
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .segment_cache(cache);
    store.reset();
    cached
        .publish_batch(
            &store,
            vec![create_dir("cached-a", "gamma")],
            &writer,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
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
        .publish_batch(
            &store,
            vec![create_dir("cached-b", "delta")],
            &writer,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
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
