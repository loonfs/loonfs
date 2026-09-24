//! Writer fencing and deletion budgets.

use super::*;
use crate::test_support::ops::create;
use loonfs_api::{AbsolutePath, WriterId};
use loonfs_objectstore::keys::hint;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass, RecordingStore};
use tempfile::tempdir;

fn context() -> MutationContext {
    MutationContext {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms: 1_000,
    }
}

#[tokio::test]
async fn deletion_budget_includes_writer_acquisition() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("deletion-budget").expect("namespace");
    let store = BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationClass::Read,
    );
    let context = context();
    create(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut publisher =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(clock.clone());
    store.block_next();
    let delete = publisher.delete_namespace(&store, Default::default(), &context);
    let delay = async {
        store.wait_until_blocked().await;
        clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        store.release();
    };
    let (result, ()) = tokio::join!(delete, delay);
    assert!(matches!(
        result,
        Err(CoreError::MetadataPublicationBudgetExceeded { .. })
    ));
    let manifest = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert!(!manifest.state.envelope.payload().status.is_deleted());
    assert_eq!(
        manifest.state.envelope.payload().manifest_no,
        loonfs_api::ManifestNo(2)
    );
}

#[tokio::test]
async fn a_stale_writer_stays_fenced_after_namespace_deletion() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("terminal-writer").expect("namespace");
    let context = context();
    create(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let mut stale = NamespaceCommitEngine::new(namespace_id.clone());
    stale
        .publish_batch(
            &store,
            [CommitCandidate::new(CommitRequest::single(
                CommitId::parse("before-delete").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/early").expect("path"),
                    parents: false,
                },
            ))],
            &context,
            &PublishTailOptions::default(),
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .remove(0)
        .expect("publish before deletion");
    NamespaceCommitEngine::new(namespace_id)
        .delete_namespace(&store, Default::default(), &context)
        .await
        .expect("delete");
    let candidate = CommitCandidate::new(CommitRequest::single(
        CommitId::parse("after-delete").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/late").expect("path"),
            parents: false,
        },
    ));
    for attempt in 0..3 {
        store.reset();
        let error = stale
            .publish_batch(
                &store,
                [candidate.clone()],
                &context,
                &PublishTailOptions::default(),
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .results
            .remove(0)
            .expect_err("stale writer");
        assert_eq!(
            error.code(),
            if attempt == 0 {
                loonfs_api::ErrorCode::StaleHead
            } else {
                loonfs_api::ErrorCode::WriterFenced
            }
        );
        assert_eq!(store.counts().puts, usize::from(attempt == 0));
        assert_eq!(store.counts().compare_and_swaps, 0);
    }
}

#[tokio::test]
async fn rejected_deletion_writes_nothing_before_folding_inline_content() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("deletion-before-fold").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = context();
    create(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut engine =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(clock.clone());
    let value = InlineContent::new(
        namespace_id.clone(),
        ContentId::generate(),
        bytes::Bytes::from_static(b"unfolded content"),
    );
    let candidate = CommitCandidate::with_inline_content(
        CommitRequest::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/content").expect("path"),
                content_ref: Some(value.content_ref().clone()),
                inline_content: None,
                behavior: loonfs_api::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        Vec::new(),
        vec![value],
    );
    engine
        .publish_batch(
            &store,
            [candidate],
            &context,
            &PublishTailOptions::default(),
            &Deadline::start(clock.clone()),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect("publish inline content");
    let acquired_writer = engine
        .session_writer_epoch(&store, &context)
        .await
        .expect("acquired writer");
    for expected_head_seq in [ChangeSeq(0), ChangeSeq(1)] {
        let deadline = Deadline::start(clock.clone());
        if expected_head_seq == ChangeSeq(1) {
            clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        }
        store.reset();
        let error = crate::namespace::delete::delete_namespace(
            &store,
            &namespace_id,
            DeleteNamespaceOptions {
                expected_head_seq: Some(expected_head_seq),
            },
            acquired_writer.clone(),
            &context,
            &deadline,
        )
        .await
        .expect_err("deletion rejected before folding");
        if expected_head_seq == ChangeSeq(0) {
            assert!(matches!(
                error,
                CoreError::StaleHeadPrecondition {
                    expected: ChangeSeq(0),
                    actual: ChangeSeq(1),
                    ..
                }
            ));
        } else {
            assert!(matches!(
                error,
                CoreError::MetadataPublicationBudgetExceeded { .. }
            ));
        }
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().compare_and_swaps, 0);
    }
}
