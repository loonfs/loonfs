//! Writer fencing and deletion budgets.

use super::*;
use crate::test_support::WriterAt;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::AbsolutePath;
use std::assert_matches;
use tempfile::tempdir;

fn context() -> WriterAt {
    crate::test_support::writer_at("writer", 1_000)
}

#[tokio::test]
async fn deletion_budget_includes_writer_acquisition() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("deletion-budget").expect("namespace");
    // The acquisition's check for a successor of its claim (manifest 3) is
    // held while the clock passes the budget.
    let store = BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::exact(metadata_manifest_object(
            &namespace_id,
            &loonfs_types::ManifestNo(4),
        )),
        OperationSelector::Head,
    );
    let context = context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut publisher = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: clock.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        context.writer_id.clone(),
    );
    store.block_next();
    let administrator = crate::test_support::administrator_of(&store, &namespace_id);
    let delete = publisher.delete_namespace(&administrator, Default::default());
    let delay = async {
        store.wait_until_blocked().await;
        clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        store.release();
    };
    let (result, ()) = tokio::join!(delete, delay);
    assert_matches!(
        result,
        Err(CoreError::MetadataPublicationBudgetExceeded { .. })
    );
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    assert!(!manifest.payload().status.is_deleted());
    assert_eq!(manifest.payload().manifest_no, loonfs_types::ManifestNo(3));
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let mut stale = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    );
    stale
        .publish_batch(
            [CommitCandidate::new(CommitIntent::single(
                CommitId::parse("before-delete").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/early").expect("path"),
                    parents: false,
                },
            ))],
            &context.clock(),
        )
        .await
        .results
        .remove(0)
        .expect("publish before deletion");
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    )
    .delete_namespace(
        &crate::test_support::administrator_of(&store, &namespace_id),
        Default::default(),
    )
    .await
    .expect("delete");
    let candidate = CommitCandidate::new(CommitIntent::single(
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
            .publish_batch([candidate.clone()], &context.clock())
            .await
            .results
            .remove(0)
            .expect_err("stale writer");
        assert_eq!(
            error.code(),
            if attempt == 0 {
                loonfs_types::ErrorCode::StaleHead
            } else {
                loonfs_types::ErrorCode::WriterFenced
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: clock.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        context.writer_id.clone(),
    );
    let value = InlineContent::new(
        ContentId::generate(),
        bytes::Bytes::from_static(b"unfolded content"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let candidate = CommitCandidate::with_inline_content(
        CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/content").expect("path"),
                content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
                behavior: loonfs_types::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        Vec::new(),
        vec![value],
    );
    engine
        .publish_batch(
            [candidate],
            &OperationClock::start(context.now_ms, clock.clone()),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect("publish inline content");
    let acquired_writer = engine
        .position
        .session_writer_epoch(&engine.namespace, &context.writer_id)
        .await
        .expect("acquired writer");
    for expected_head_seq in [CommitSeq(0), CommitSeq(1)] {
        let deadline = OperationClock::start(context.now_ms, clock.clone());
        if expected_head_seq == CommitSeq(1) {
            clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        }
        store.reset();
        let error = crate::test_support::namespace_store(&store, &namespace_id)
            .delete_namespace(
                DeleteNamespaceOptions {
                    expected_head_seq: Some(expected_head_seq),
                },
                acquired_writer.clone(),
                None,
                &deadline,
            )
            .await
            .expect_err("deletion rejected before folding");
        if expected_head_seq == CommitSeq(0) {
            assert_matches!(
                error,
                CoreError::StaleHeadPrecondition {
                    expected: CommitSeq(0),
                    actual: CommitSeq(1),
                    ..
                }
            );
        } else {
            assert_matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. });
        }
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().compare_and_swaps, 0);
    }
}

/// Checks that a deletion whose tombstone put loses its response returns the
/// deletion's response when the put was written, and `namespace_deleted` when
/// another deletion by the same writer published the same tombstone content
/// at that number first.
#[tokio::test]
async fn a_deletion_whose_tombstone_put_loses_its_response_reads_back_whose_tombstone_it_is() {
    use crate::namespace::writer_epoch::AcquiredWriter;
    use loonfs_test_support::stores::{FailStore, InjectedError};

    for rival in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("lost-tombstone").expect("namespace");
        let base = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let context = context();
        crate::test_support::NamespaceFixture::new(&base, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        // The deletion claims the writer epoch at manifest 3, folds the
        // claim's fence into manifest 4, and puts its tombstone at manifest 5.
        let tombstone = metadata_manifest_object(&namespace_id, &loonfs_types::ManifestNo(5));
        let failing = FailStore::new(
            base.clone(),
            KeyPredicate::exact(&tombstone),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost response".to_owned()),
        );
        // Without a rival, the put is written and its response is lost. With
        // one, the put waits while the rival publishes, and then fails before
        // it reaches the store.
        let failing = if rival {
            failing
        } else {
            failing.apply_then_fail()
        };
        failing.fail_next(1);
        let store = BlockingStore::new(
            failing,
            KeyPredicate::exact(&tombstone),
            OperationSelector::PutCreateIfAbsent,
        );
        if rival {
            store.block_next();
        }
        let mut engine = NamespaceCommitEngine::new(
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context).namespace,
            context.writer_id.clone(),
        );
        let administrator = crate::test_support::administrator_of(&store, &namespace_id);
        let deleted = engine.delete_namespace(&administrator, Default::default());
        let (deleted, ()) = tokio::join!(deleted, async {
            if !rival {
                return;
            }
            store.wait_until_blocked().await;
            let other = crate::test_support::NamespaceFixture::new(&base, &namespace_id, &context);
            other
                .namespace
                .delete_namespace(
                    Default::default(),
                    AcquiredWriter {
                        writer_id: context.writer_id.clone(),
                        writer_epoch: WriterEpoch(1),
                    },
                    None,
                    &other.namespace.start_clock(),
                )
                .await
                .expect("the rival deletion");
            store.release();
        });
        assert_eq!(store.inner().remaining(), 0);
        if rival {
            assert_matches!(deleted, Err(CoreError::NamespaceDeleted { .. }));
        } else {
            assert_eq!(deleted.expect("the deletion").namespace_id, namespace_id);
        }
        let current = crate::test_support::namespace_store(&base, &namespace_id)
            .find_newest_manifest(crate::test_support::observed_now())
            .await
            .expect("newest manifest");
        let crate::namespace::control::NewestManifest::Manifest(current) = current else {
            panic!("the tombstone is the newest manifest");
        };
        assert_eq!(
            current.manifest().payload().manifest_no,
            loonfs_types::ManifestNo(5)
        );
        assert!(current.manifest().payload().status.is_deleted());
    }
}
