//! Numbered publication and retention contracts.

use super::*;
use std::assert_matches;

/// Checks that a manifest put that returns after its publication budget,
/// at a number above the manifest boundary, is published: the boundary read
/// after the put is the check of the put, and no budget check follows it.
#[tokio::test]
async fn a_manifest_put_returning_after_its_budget_is_published() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("manifest-return-budget").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
    let object_key = metadata_manifest_object(&namespace_id, &payload.manifest_no);
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let deadline = OperationClock::start(0, timer.clone());
    store.reset();
    let store = loonfs_test_support::stores::MetadataMapStore::new(
        store,
        KeyPredicate::exact(&object_key),
        move |metadata| {
            timer.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
            metadata
        },
    );
    let outcome = publish_once_within(&store, payload, &deadline)
        .await
        .expect("a late put above the boundary");
    assert_matches!(outcome, ManifestPublicationOutcome::Published(_));
    let operations = store.inner().snapshot();
    assert_eq!(
        operations
            .iter()
            .rev()
            .map(|operation| operation.key().to_owned())
            .take(2)
            .collect::<Vec<_>>(),
        [boundary(&namespace_id), object_key]
    );
}

#[tokio::test]
async fn a_manifest_put_that_writes_nothing_and_loses_its_answer_has_an_unknown_outcome() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
    let object_key = metadata_manifest_object(&namespace_id, &payload.manifest_no);
    let store = FailStore::new(
        store,
        KeyPredicate::exact(&object_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost request".to_owned()),
    );
    store.fail_next(1);
    let error = publish_once(&store, payload)
        .await
        .expect_err("a put nobody can confirm has no answer");
    assert_eq!(error.code(), ErrorCode::OutcomeUnknown);
    assert_matches!(
        error,
        CoreError::OutcomeUnknown { object_key: actual_key, .. } if actual_key == object_key
    );
    assert!(store
        .inner()
        .head(&object_key)
        .await
        .expect("head")
        .is_none());
}

/// Checks that a manifest put whose response is lost, and whose read-back
/// ends after the publication budget, is settled by the read-back: the
/// manifest found at its number is its own, for a fork's first manifest and
/// for a successor.
#[tokio::test]
async fn an_ambiguous_manifest_read_back_after_its_budget_settles_the_put() {
    use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_test_support::stores::MetadataMapStore;
    use loonfs_types::format::control::ForkBasis;

    for fork in [false, true] {
        let elapsed_ms = METADATA_PUBLICATION_BUDGET_MS + 1;
        let directory = tempdir().expect("directory");
        let source = NamespaceId::parse("source").expect("namespace");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        crate::test_support::NamespaceFixture::new(&store, &source, &test_context())
            .create()
            .await
            .expect("create");
        let current = crate::test_support::namespace_store(&store, &source)
            .load_current_manifest()
            .await
            .expect("current");
        let mut payload = current.payload().clone();
        if fork {
            payload.namespace_id = NamespaceId::parse("fork").expect("namespace");
            payload.fork_basis = Some(ForkBasis {
                manifest: current.manifest(),
                fork_point_seq: current.manifest().head_seq,
                source_snapshot_id: SnapshotId::generate(current.manifest().manifest_no),
                forked_from_snapshot_id: SnapshotId::generate(current.manifest().manifest_no),
                fork_point_snapshot_id: SnapshotId::generate(ManifestNo(1)),
                rebased_from: None,
            });
            payload.status = loonfs_types::format::control::NamespaceStatus::Initializing {};
            payload.manifest_no = ManifestNo(1);
        } else {
            payload.manifest_no = ManifestNo(3);
        }
        let object_key = metadata_manifest_object(&payload.namespace_id, &payload.manifest_no);
        let timer = Arc::new(ManualClock::new(0));
        let deadline = OperationClock::start(0, timer.clone());
        store.reset();
        let store =
            MetadataMapStore::new(store, KeyPredicate::exact(&object_key), move |metadata| {
                timer.advance_ms(elapsed_ms);
                metadata
            });
        let store = FailStore::new(
            store,
            KeyPredicate::exact(&object_key),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost acknowledgement".to_owned()),
        )
        .apply_then_fail();
        store.fail_next(1);
        // A fork's first manifest is created, and any other manifest
        // is published over its predecessor.
        let outcome = if fork {
            let namespace = crate::test_support::namespace_store(&store, &payload.namespace_id);
            namespace
                .publish_first_manifest(payload, &deadline)
                .await
                .map(|_| None)
        } else {
            publish_once_within(&store, payload, &deadline)
                .await
                .map(Some)
        };
        if fork {
            assert_matches!(outcome, Ok(None));
        } else {
            assert_matches!(outcome, Ok(Some(ManifestPublicationOutcome::Published(_))));
        }
        assert_eq!(store.remaining(), 0);
        assert_eq!(store.inner().inner().counts().create_if_absent_puts, 1);
        assert_eq!(store.inner().inner().counts().compare_and_swaps, 0);
        assert!(store
            .inner()
            .inner()
            .take_gets()
            .iter()
            .any(|(key, _)| key == &object_key));
    }
}

/// Checks that a manifest put that succeeds, followed by a read of the
/// manifest boundary that fails, returns `outcome_unknown` for the
/// manifest's key.
#[tokio::test]
async fn a_failed_boundary_read_after_a_put_has_an_unknown_outcome() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(boundary(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("boundary read failed".to_owned()),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("bootstrap");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let basis = namespace
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("current");
    let mut payload = basis.manifest().payload().clone();
    payload.manifest_no = ManifestNo(3);
    store.fail_next(1);
    let outcome = namespace
        .publish_manifest_from(
            payload,
            &OperationClock::start(0, crate::test_support::std_timer()),
            Some(&basis),
        )
        .await;
    let object_key = metadata_manifest_object(&namespace_id, &ManifestNo(3));
    assert_matches!(
        outcome,
        Err(CoreError::OutcomeUnknown { object_key: actual, .. }) if actual == object_key
    );
    assert_eq!(store.remaining(), 0);
    assert!(store
        .inner()
        .head(&object_key)
        .await
        .expect("the put")
        .is_some());
}

#[tokio::test]
async fn manifest_publication_recovers_an_ambiguous_put() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let mut payload = current.payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next");
    let candidate_manifest_no = payload.manifest_no;
    let store = FailStore::new(
        store,
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Put,
        InjectedError::Transport("lost acknowledgment".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    let outcome = publish_once(&store, payload).await.expect("published");
    // The manifest that the read-back finds has the put's publication ID.
    assert_matches!(outcome, ManifestPublicationOutcome::Published(_));
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("discover")
            .manifest()
            .manifest_no,
        candidate_manifest_no
    );
}

#[tokio::test]
async fn a_fold_whose_manifest_put_succeeds_without_an_answer_reports_published() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost acknowledgement".to_owned()),
    )
    .apply_then_fail();
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/file", b"data", None)
        .await
        .expect("write");
    // The session claims before the fault is armed, so the fault is the
    // fold's own manifest put.
    let acquired = crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer(&context.writer_id)
        .await
        .expect("claim");
    let mut session = NamespaceCommitEngine::with_acquired_session(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
        acquired.writer,
        acquired.claim,
    );
    let predecessor = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current")
        .manifest()
        .manifest_no;
    store.fail_next(1);
    let folded = session.fold().await.expect("fold").response;
    assert_eq!(store.remaining(), 0);
    assert_eq!(folded.outcome, crate::FoldWalOutcome::Published);
    assert_eq!(
        folded.manifest_no,
        predecessor.successor().expect("next number")
    );
}

#[tokio::test]
async fn namespace_status_and_change_feed_report_the_floor_at_the_head_after_an_advance() {
    // Retention publishes the floor independently of the head. A reader must
    // not report a floor newer than its head.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    create_snapshot(&inner, &namespace_id, &context)
        .await
        .expect("create snapshot");
    crate::test_support::NamespaceFixture::new(
        &inner,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance retention");

    let namespace = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_metadata()
        .await
        .expect("status");
    assert_eq!(namespace.head_seq, CommitSeq(1));
    assert_eq!(namespace.retention_floor_seq, CommitSeq(1));

    let namespace = crate::test_support::namespace_store(&inner, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load feed view");
    let changes = list_changes_after(
        &view,
        loonfs_types::ChangeFeedPosition::after(CommitSeq(1)),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
        loonfs_types::options::CommitSplitting::Whole,
    )
    .await
    .expect("change feed");
    assert_eq!(changes.head_seq, CommitSeq(1));
    assert!(changes.changes.is_empty());
}

/// Checks that a retention window change whose put was written but lost its
/// response finds its own publication ID at the read-back, and publishes
/// once.
#[tokio::test]
async fn an_ambiguous_window_change_confirms_its_own_put() {
    use loonfs_test_support::stores::{FailStore, InjectedError};

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("window-change").expect("namespace");
    // The change's session claims the namespace at manifest 3, so the change
    // is manifest 4.
    let change_key = metadata_manifest_object(&namespace_id, &ManifestNo(4));
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(move |key| key == change_key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("response lost".to_owned()),
    )
    .apply_then_fail();
    store.fail_next(1);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    // The change was written, and the read-back finds the change's own
    // publication ID, so the change publishes manifest 4 and no other.
    let changed = crate::test_support::change_retention_window(
        &crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine(),
    )
    .await;
    assert_eq!(store.remaining(), 0, "the change's put lost its response");
    assert_eq!(changed.manifest().payload().manifest_no, ManifestNo(4));
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert_eq!(current.manifest(), changed.manifest().manifest());
}

/// Checks that two puts of one payload at one number store different bytes,
/// which differ only in the publication ID.
#[tokio::test]
async fn two_puts_of_one_payload_store_different_bytes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let basis = namespace
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("current");
    let mut payload = basis.manifest().payload().clone();
    payload.manifest_no = payload.manifest_no.successor().expect("next manifest");
    let mut published = Vec::new();
    for _ in 0..2 {
        let outcome = namespace
            .publish_manifest_from(
                payload.clone(),
                &OperationClock::start(0, crate::test_support::std_timer()),
                Some(&basis),
            )
            .await
            .expect("publish");
        let ManifestPublicationOutcome::Published(outcome) = outcome else {
            panic!("the put is published, got {outcome:?}");
        };
        published.push(outcome.into_manifest());
        // The number is free again for the second put.
        store
            .delete(&metadata_manifest_object(
                &namespace_id,
                &payload.manifest_no,
            ))
            .await
            .expect("delete");
    }
    let [first, second] = &published[..] else {
        panic!("two publications");
    };
    assert_ne!(first.manifest(), second.manifest());
    assert_ne!(
        first.payload().publication_id,
        second.payload().publication_id
    );
    let mut second_payload = second.payload().clone();
    second_payload.publication_id = first.payload().publication_id.clone();
    assert_eq!(first.payload(), &second_payload);
}

/// Checks that a successor built as a copy of its predecessor's payload is
/// published with a publication ID of its own.
#[tokio::test]
async fn a_successor_that_copies_its_predecessor_gets_its_own_publication_id() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let basis = namespace
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("current");
    let predecessor = basis.manifest().clone();
    let outcome = namespace
        .publish_manifest_from(
            predecessor.payload().clone(),
            &OperationClock::start(0, crate::test_support::std_timer()),
            Some(&basis),
        )
        .await
        .expect("publish a copy");
    let ManifestPublicationOutcome::Published(published) = outcome else {
        panic!("the copy is published, got {outcome:?}");
    };
    let successor = namespace.load_current_manifest().await.expect("current");
    assert_eq!(successor.manifest(), published.manifest().manifest());
    assert_eq!(
        successor.payload().manifest_no,
        predecessor.payload().manifest_no.successor().expect("next")
    );
    assert_ne!(
        successor.payload().publication_id,
        predecessor.payload().publication_id
    );
}
