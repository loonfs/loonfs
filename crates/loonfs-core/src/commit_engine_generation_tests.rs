//! Publication receipts and deletion budgets across generations.

use super::*;
use crate::namespace::bootstrap::bootstrap_namespace;
use loonfs_api::{AbsolutePath, NamespaceAccess, WriterId};
use loonfs_objectstore::keys::{hint, metadata_manifest_object};
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
async fn retained_receipts_revalidate_once_per_interval_and_fence_after_recreation() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("receipt-generation").expect("namespace");
    let context = context();
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut publisher =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(clock.clone());
    let candidate = CommitCandidate::new(CommitRequest::single(
        CommitId::parse("retained").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/directory").expect("path"),
            parents: false,
        },
    ));
    let options = PublishTailOptions::default();
    let committed = publisher
        .publish_batch(&store, [candidate.clone()], &context, &options)
        .await
        .results
        .remove(0)
        .expect("commit");
    let successor_key = metadata_manifest_object(
        &namespace_id,
        &publisher
            .publish_tail_projection
            .as_ref()
            .expect("projection")
            .basis()
            .manifest_no()
            .successor()
            .expect("successor"),
    );
    store.reset();
    clock.advance_ms(1_000);
    for _ in 0..2 {
        assert_eq!(
            publisher
                .publish_batch(&store, [candidate.clone()], &context, &options)
                .await
                .results
                .remove(0)
                .expect("replay"),
            committed
        );
    }
    assert_eq!(store.counts().heads, 1);
    assert!(store.snapshot().iter().any(|operation| matches!(operation, loonfs_test_support::stores::RecordedOperation::Head { key, .. } if key == &successor_key)));
    NamespaceCommitEngine::new(namespace_id.clone())
        .delete_namespace(&store, Default::default(), &context)
        .await
        .expect("delete");
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect("recreate");
    clock.advance_ms(1_000);
    store.reset();
    assert!(matches!(
        publisher
            .publish_batch(&store, [candidate.clone()], &context, &options)
            .await
            .results
            .remove(0),
        Err(CoreError::WriterFenced(_))
    ));
    assert_eq!(store.counts().puts, 0);
    assert!(publisher.retained_tail_weight().is_none());
    let mut current = NamespaceCommitEngine::new(namespace_id);
    assert_eq!(
        current
            .publish_batch(&store, [candidate], &context, &options)
            .await
            .results
            .remove(0)
            .expect("new generation commit")
            .committed_seq,
        ChangeSeq(1)
    );
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context,
        &loonfs_test_support::test_actor(),
        &NamespaceAccess::unrestricted(),
        false,
    )
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
    assert!(!manifest.envelope.payload().status.is_deleted());
    assert_eq!(
        manifest.envelope.payload().manifest_no,
        loonfs_api::ManifestNo(2)
    );
}
