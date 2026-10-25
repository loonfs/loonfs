//! A writer session's expiry of the deletions that a retention advance
//! reaches, against undeletes that the session validates around it.

use super::*;
use crate::commit::CommitValidationError;
use crate::manifest::{advance_retention_floor, RetentionTarget};
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{FailStore, InjectedError, OperationContext, OperationKind};
use loonfs_types::api::v0::FilesystemChange;
use loonfs_types::format::wal::{decode_wal_object_envelope_zstd, WalDelta};
use loonfs_types::{AbsolutePath, ActorId, DeleteDirectoryBehavior, ExpiredDeletion, InodeId};
use std::assert_matches;

fn operation(commit_id: &str, operation: FilesystemOperation) -> CommitCandidate {
    CommitCandidate::new(CommitIntent::single(
        CommitId::parse(commit_id).expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        operation,
    ))
}

fn path(absolute_path: &str) -> AbsolutePath {
    AbsolutePath::parse(absolute_path).expect("path")
}

fn undelete(commit_id: &str, inode_id: InodeId, deletion_seq: ChangeSeq) -> CommitCandidate {
    operation(
        commit_id,
        FilesystemOperation::Undelete {
            inode_id,
            deletion_seq,
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
        },
    )
}

async fn publish<S: ObjectStore + ?Sized>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    candidate: CommitCandidate,
) -> Result<Commit> {
    engine
        .publish_batch(
            store,
            [candidate],
            &context("writer"),
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .pop()
        .expect("one result")
}

/// Deletes `/name` and returns the deleted root and the deletion's sequence.
async fn delete<S: ObjectStore + ?Sized>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    name: &str,
) -> (InodeId, ChangeSeq) {
    publish(
        engine,
        store,
        operation(
            &format!("create-{name}"),
            FilesystemOperation::CreateDirectory {
                path: path(&format!("/{name}")),
                parents: false,
            },
        ),
    )
    .await
    .expect("create directory");
    let deleted = publish(
        engine,
        store,
        operation(
            &format!("delete-{name}"),
            FilesystemOperation::DeletePath {
                path: path(&format!("/{name}")),
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
        ),
    )
    .await
    .expect("delete directory");
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("a delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

#[tokio::test]
async fn an_undelete_before_the_expiry_step_revokes_its_deletion_and_one_after_returns_not_deleted()
{
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("expiry").expect("valid namespace id");
    let writer = context("writer");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    let (restored, restored_seq) = delete(&mut engine, &store, "restored").await;
    let (expired, expired_seq) = delete(&mut engine, &store, "expired").await;
    let deadline = || Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let folded = fold_wal_tail(
        &store,
        None,
        &namespace_id,
        engine.wal_fold_input(),
        &deadline(),
        crate::manifest::MetadataRunPolicy::default(),
        &crate::test_support::content_writes(),
    )
    .await
    .expect("fold");
    engine.record_manifest_published(folded.manifest);
    let advanced = advance_retention_floor(
        &store,
        None,
        &namespace_id,
        RetentionTarget::Head,
        crate::test_support::grace_elapsed_after(1_000),
        Arc::new(StdMonotonicTimer::default()),
    )
    .await
    .expect("advance the floor");
    assert_eq!(advanced.response.retention_floor_seq, expired_seq);
    assert!(!engine.expiry_due());
    engine.record_manifest_published(advanced.published.expect("a published advance"));
    assert!(engine.expiry_due(), "the move onto the new floor");

    // The session validates this undelete before the step, so the undelete
    // revokes the deletion, and the step's scan doesn't find it.
    publish(
        &mut engine,
        &store,
        undelete("undelete-before", restored, restored_seq),
    )
    .await
    .expect("an undelete before the step");
    let step = engine
        .publish_expiry_step(&store, &writer, &deadline())
        .await
        .expect("a step with a deletion to expire")
        .results
        .pop()
        .expect("one result")
        .expect("the expiry commit");
    assert_eq!(step.committed_by, ActorId::loonfs());
    assert_eq!(
        step.events,
        vec![FilesystemChange::DeletionExpired {
            inode_id: expired,
            deletion_seq: expired_seq,
        }]
    );
    assert_matches!(
        engine
            .publish_expiry_step(&store, &writer, &deadline())
            .await,
        None,
        "no deletion at or below the floor is left"
    );
    assert!(!engine.expiry_due());

    let error = publish(
        &mut engine,
        &store,
        undelete("undelete-after", expired, expired_seq),
    )
    .await
    .expect_err("an undelete after the step");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
}

#[tokio::test]
async fn an_expiry_of_a_deletion_above_the_floor_fails_validation_and_commits_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("expiry-floor").expect("valid namespace id");
    let writer = context("writer");
    create(&store, &namespace_id, &writer)
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    let (inode_id, deletion_seq) = delete(&mut engine, &store, "above-floor").await;

    // The floor is still 0, so the deletion is above it.
    let error = engine
        .publish_expiry(
            &store,
            CommitId::generate(),
            vec![ExpiredDeletion {
                inode_id,
                deletion_seq,
            }],
            &writer,
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect_err("an expiry above the floor");
    assert_matches!(
        &error,
        CoreError::FailedOperation { error, .. } if matches!(
            **error,
            CoreError::CommitValidation(CommitValidationError::DeletionAboveRetentionFloor {
                deletion_seq: rejected_seq,
                retention_floor_seq: ChangeSeq(0),
                ..
            }) if rejected_seq == deletion_seq
        ),
        "{error:?}"
    );
    assert_eq!(error.code(), ErrorCode::ServerError);
    let head = load_namespace_read_state(&store, &namespace_id, crate::test_support::std_timer())
        .await
        .expect("read head");
    assert_eq!(
        head.seq, deletion_seq,
        "the rejected expiry commits nothing"
    );
    publish(
        &mut engine,
        &store,
        undelete("undelete-above-floor", inode_id, deletion_seq),
    )
    .await
    .expect("the deletion stays recoverable");
}

/// Whether `operation` is the WAL put of an expiry commit.
fn is_expiry_put(operation: &OperationContext<'_>) -> bool {
    matches!(
        operation.kind(),
        OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
            if decode_wal_object_envelope_zstd(bytes).is_ok_and(|wal| {
                wal.payload().records.iter().any(|record| {
                    record
                        .deltas
                        .iter()
                        .any(|delta| matches!(delta.delta, WalDelta::ExpireSubtreeTombstone { .. }))
                })
            })
    )
}

/// Folds the session's tail, advances the floor to the folded head, and
/// moves the session's position onto the advance.
async fn advance_the_floor<S: ObjectStore + ?Sized>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    namespace_id: &NamespaceId,
) {
    let folded = fold_wal_tail(
        store,
        None,
        namespace_id,
        engine.wal_fold_input(),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &crate::test_support::content_writes(),
    )
    .await
    .expect("fold");
    engine.record_manifest_published(folded.manifest);
    let advanced = advance_retention_floor(
        store,
        None,
        namespace_id,
        RetentionTarget::Head,
        crate::test_support::grace_elapsed_after(1_000),
        Arc::new(StdMonotonicTimer::default()),
    )
    .await
    .expect("advance the floor");
    engine.record_manifest_published(advanced.published.expect("a published advance"));
}

/// Publishes the creation of `/write-{index}`.
async fn create_directory<S: ObjectStore + ?Sized>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    index: usize,
) {
    publish(
        engine,
        store,
        operation(
            &format!("write-{index}"),
            FilesystemOperation::CreateDirectory {
                path: path(&format!("/write-{index}")),
                parents: false,
            },
        ),
    )
    .await
    .expect("write");
}

/// Runs one expiry step and reports whether its commit was published.
async fn step<S: ObjectStore + ?Sized>(engine: &mut NamespaceCommitEngine, store: &S) -> bool {
    engine
        .publish_expiry_step(
            store,
            &context("writer"),
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .expect("a step with a deletion to expire")
        .results
        .iter()
        .all(Result::is_ok)
}

#[tokio::test]
async fn failed_expiry_steps_wait_for_doubling_publications_and_a_success_resets_the_wait() {
    let temp_dir = tempdir().expect("tempdir");
    let store = FailStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        is_expiry_put,
        InjectedError::Transport("the store refuses the expiry".to_owned()),
    );
    let namespace_id = NamespaceId::parse("expiry-backoff").expect("valid namespace id");
    create(&store, &namespace_id, &context("writer"))
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    delete(&mut engine, &store, "first").await;
    advance_the_floor(&mut engine, &store, &namespace_id).await;
    // Each failure doubles the publications the session waits for, so the
    // steps after the first come after writes 1, 3, 7, and 15.
    store.fail_all();
    assert!(!step(&mut engine, &store).await, "the first step fails");
    let mut retried_after = Vec::new();
    let mut written = 0;
    while retried_after.len() < 4 {
        written += 1;
        create_directory(&mut engine, &store, written).await;
        if engine.expiry_due() {
            if retried_after.len() == 3 {
                store.clear();
            }
            retried_after.push((written, step(&mut engine, &store).await));
        }
    }
    assert_eq!(
        retried_after,
        vec![(1, false), (3, false), (7, false), (15, true)]
    );
    assert_eq!(store.attempts(), 5);
    assert_matches!(
        engine
            .publish_expiry_step(
                &store,
                &context("writer"),
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await,
        None,
        "the success expired the deletion"
    );
    assert!(!engine.expiry_due());

    // The success reset the count, so the next failure waits one
    // publication, not sixteen.
    delete(&mut engine, &store, "second").await;
    advance_the_floor(&mut engine, &store, &namespace_id).await;
    store.fail_all();
    assert!(
        !step(&mut engine, &store).await,
        "a step after the reset fails"
    );
    assert!(!engine.expiry_due());
    create_directory(&mut engine, &store, written + 1).await;
    assert!(engine.expiry_due(), "one publication ends the wait");
    store.clear();
    assert!(step(&mut engine, &store).await, "the retry publishes");
}
