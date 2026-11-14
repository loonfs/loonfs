//! Races between garbage collection and the two records of an upload
//! session: a collection that stops between its two deletes, and a completion or
//! abort that stalls while a collection deletes the session.

use super::*;
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::keys::{
    content_blob, upload_outcome, upload_session, upload_session_prefix,
};
use loonfs_types::CompleteUploadBody;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Commits `prepared` at `path`.
async fn commit_upload<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    prepared: PreparedContent,
) {
    let content_ref = prepared.content_ref().clone();
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        context(2_000).writer_id.clone(),
    )
    .publish_batch(
        vec![CommitCandidate::prepared(
            CommitIntent::single(
                loonfs_types::CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: loonfs_types::AbsolutePath::parse(path).expect("path"),
                    content: loonfs_types::FileContent::Ref(content_ref),
                    behavior: loonfs_types::DestinationBehavior::Replace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            ),
            vec![prepared],
        )],
        &context(2_000).clock(),
    )
    .await
    .results
    .pop()
    .expect("one result")
    .expect("published");
}

async fn complete_proxied<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    now_ms: u64,
) -> crate::error::Result<crate::upload::complete::CompletedUpload> {
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context(now_ms))
        .engine()
        .complete_upload(upload_id, CompleteUploadBody::ServiceProxied {})
        .await
}

async fn abort<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    now_ms: u64,
) -> crate::error::Result<loonfs_types::UploadSession> {
    let catalog = crate::test_support::namespace_store(store, namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    crate::test_support::NamespaceFixture::new(store, catalog.namespace_id(), &context(now_ms))
        .engine()
        .abort_upload(upload_id)
        .await
}

async fn upload_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    let pool_id = crate::test_support::load_pool_id(store, namespace_id).await;
    store
        .list_prefix(&upload_session_prefix(&pool_id))
        .await
        .expect("list upload records")
}

async fn collect_at<S: ObjectStore>(
    store: &S,
    pool_id: &PoolId,
    now_ms: u64,
) -> crate::error::Result<PoolGcResponse> {
    crate::test_support::pool_store(store, pool_id)
        .gc(None, &context(now_ms).clock())
        .await
}

/// Which upload outcome a blocked put-if-absent creates.
#[derive(Debug, Clone, Copy)]
enum BlockedOutcome {
    Completed,
    Aborted,
}

impl BlockedOutcome {
    fn matches(self, bytes: &[u8]) -> bool {
        let Ok(envelope) =
            decode_control_object::<UploadOutcomePayload>(bytes, ControlObjectKind::UploadOutcome)
        else {
            return false;
        };
        match (self, &envelope.payload().outcome) {
            (Self::Completed, UploadOutcome::Completed { .. })
            | (Self::Aborted, UploadOutcome::Aborted { .. }) => true,
            (Self::Completed, UploadOutcome::Aborted { .. })
            | (Self::Aborted, UploadOutcome::Completed { .. }) => false,
        }
    }
}

/// Blocks the next put that would create an upload outcome record of `target`.
fn blocking_outcome_store(
    inner: LocalFsStore,
    target: BlockedOutcome,
) -> BlockingStore<LocalFsStore> {
    let store =
        BlockingStore::matching(
            inner,
            move |operation: &OperationContext<'_>| match operation.kind() {
                OperationKind::Put {
                    bytes,
                    mode: PutMode::CreateIfAbsent,
                } => target.matches(bytes),
                _ => false,
            },
        );
    store.block_next();
    store
}

/// A collection that stops between deleting a session's two records leaves the
/// outcome record without its session record. The next collection deletes that
/// outcome and keeps the committed content. If the outcome record were
/// deleted first, the session record left behind would read as an open,
/// expired session, and the next collection would abort it and delete the content.
#[tokio::test]
async fn a_collection_that_stops_between_the_two_deletes_leaves_an_outcome_the_next_collection_removes(
) {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let upload_deletes = Arc::new(AtomicUsize::new(0));
    // Fails the second delete of an upload record, whichever record it is.
    let store = FailStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        {
            let upload_deletes = Arc::clone(&upload_deletes);
            move |operation: &OperationContext<'_>| {
                KeyPredicate::upload_records().matches(operation.key())
                    && matches!(operation.kind(), OperationKind::Delete)
                    && upload_deletes.fetch_add(1, Ordering::SeqCst) == 1
            }
        },
        InjectedError::Transport("the process stopped".to_owned()),
    );
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let completed = complete_proxied(&store, &namespace_id, &upload_id, 2_000)
        .await
        .expect("complete");
    commit_upload(&store, &namespace_id, "/committed.txt", completed.prepared).await;
    let content_key = content_blob(&pool_id, &content_ref.content_id);
    let past_grace = now_after_newest_object(
        &store,
        &namespace_id,
        CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1,
    )
    .await;

    upload_deletes.store(0, Ordering::SeqCst);
    store.fail_next(1);
    collect_at(&store, &pool_id, past_grace)
        .await
        .expect_err("the collection stops at its second delete");
    assert_eq!(
        upload_keys(&store, &namespace_id).await,
        [upload_outcome(&pool_id, &upload_id)],
        "the session record goes first"
    );
    assert!(read_upload_session(&store, &pool_id, &upload_id)
        .await
        .is_none());

    let report = collect_at(&store, &pool_id, past_grace)
        .await
        .expect("the next collection");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(report.deleted.content_objects, 0);
    assert!(upload_keys(&store, &namespace_id).await.is_empty());
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "the committed content stays"
    );
    let report = collect_at(&store, &pool_id, past_grace)
        .await
        .expect("a later collection");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());
}

/// A completion that read the session as open, and then stalled while the
/// session was aborted and a collection deleted its records, creates an outcome
/// record for a session that no longer exists. The completion finds the
/// session record gone and refuses, so it returns no token, and the next
/// collection deletes the outcome it left.
#[tokio::test]
async fn a_completion_stalled_across_a_collection_that_deleted_an_aborted_session_is_refused() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let store = blocking_outcome_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        BlockedOutcome::Completed,
    );
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let content_key = content_blob(&pool_id, &content_ref.content_id);
    let aborted_at_ms = 3_000;
    let past_abort_grace = aborted_at_ms + GC_GRACE_WINDOW_MS + 1;

    let (completion, ()) = tokio::join!(
        complete_proxied(&store, &namespace_id, &upload_id, 2_000),
        async {
            store.wait_until_blocked().await;
            abort(store.inner(), &namespace_id, &upload_id, aborted_at_ms)
                .await
                .expect("abort while the completion is stalled");
            let report = collect_at(store.inner(), &pool_id, past_abort_grace)
                .await
                .expect("a collection deletes the aborted session");
            assert_eq!(report.deleted.upload_sessions, 1);
            assert!(upload_keys(store.inner(), &namespace_id).await.is_empty());
            store.release();
        }
    );

    assert_matches!(completion, Err(CoreError::UploadNotFound { .. }));
    assert_eq!(
        upload_keys(&store, &namespace_id).await,
        [upload_outcome(&pool_id, &upload_id)],
        "the stalled completion's outcome is left without its session"
    );
    for result in [
        complete_proxied(&store, &namespace_id, &upload_id, 4_000)
            .await
            .map(drop),
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 4_000),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .map(drop),
    ] {
        assert_matches!(result, Err(CoreError::UploadNotFound { .. }));
    }
    // The leftover outcome is deleted once it's older than the grace window.
    let outcome_aged = now_after_newest_object(&store, &namespace_id, GC_GRACE_WINDOW_MS).await;
    let report = collect_at(&store, &pool_id, outcome_aged)
        .await
        .expect("the next collection");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(upload_keys(&store, &namespace_id).await.is_empty());
    assert!(store.head(&content_key).await.expect("head").is_none());
}

/// An outcome record without its session record is deleted only once it's
/// at least the grace window old by its listing time. A collection can't tell a
/// leftover outcome from the new outcome of a session whose session record
/// its listing missed, so a young one survives.
#[tokio::test]
async fn an_outcome_without_its_session_record_is_deleted_only_once_older_than_the_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let pool_id =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
            .create()
            .await
            .expect("bootstrap")
            .pool_id;
    let session = crate::test_support::proxied_upload_session(&namespace_id, 2_000);
    let session_key = crate::test_support::write_upload_records(
        &store,
        &pool_id,
        &session,
        Some(UploadOutcome::Aborted {
            aborted_at_ms: 1_000,
        }),
    )
    .await;
    store
        .delete(&session_key)
        .await
        .expect("delete session record");
    let outcome_key = upload_outcome(&pool_id, &session.upload_id);
    let written_at_ms = store
        .head(&outcome_key)
        .await
        .expect("head")
        .expect("outcome")
        .last_modified_at_ms
        .expect("the local store reports modification times");

    let young = collect_at(&store, &pool_id, written_at_ms + GC_GRACE_WINDOW_MS - 1)
        .await
        .expect("a collection inside the grace window");
    assert_eq!(young.deleted.upload_sessions, 0);
    assert_eq!(upload_keys(&store, &namespace_id).await, [outcome_key]);

    let aged = collect_at(&store, &pool_id, written_at_ms + GC_GRACE_WINDOW_MS)
        .await
        .expect("a collection at the grace window");
    assert_eq!(aged.deleted.upload_sessions, 1);
    assert!(upload_keys(&store, &namespace_id).await.is_empty());
}

/// An abort that read the session as open, and then stalled while the
/// session completed, its content was committed, and a collection deleted the
/// session's records after the content grace, would otherwise delete
/// committed content. The abort finds the session record gone and deletes
/// nothing.
#[tokio::test]
async fn an_abort_stalled_across_a_collection_that_deleted_a_completed_session_keeps_its_content() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let store = blocking_outcome_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        BlockedOutcome::Aborted,
    );
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let content_key = content_blob(&pool_id, &content_ref.content_id);

    let (aborted, ()) = tokio::join!(abort(&store, &namespace_id, &upload_id, 2_000), async {
        store.wait_until_blocked().await;
        let completed = complete_proxied(store.inner(), &namespace_id, &upload_id, 2_000)
            .await
            .expect("complete while the abort is stalled");
        commit_upload(
            store.inner(),
            &namespace_id,
            "/committed.txt",
            completed.prepared,
        )
        .await;
        let past_grace = now_after_newest_object(
            store.inner(),
            &namespace_id,
            CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1,
        )
        .await;
        let report = collect_at(store.inner(), &pool_id, past_grace)
            .await
            .expect("a collection deletes the completed session");
        assert_eq!(report.deleted.upload_sessions, 1);
        store.release();
    });

    assert_matches!(aborted, Err(CoreError::UploadNotFound { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "the stalled abort deletes no committed content"
    );
    assert!(store
        .head(&upload_session(&pool_id, &upload_id))
        .await
        .expect("head")
        .is_none());
}

#[tokio::test]
async fn upload_completion_wins_before_gc_abort_and_the_session_is_retained() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let aged = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GC_GRACE_WINDOW_MS + 1);
    let content_key = loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id);
    let store = blocking_outcome_store(store, BlockedOutcome::Aborted);
    let pool = crate::test_support::pool_store(&store, &pool_id);
    let clock = aged.clock();
    let gc = pool.gc(None, &clock);
    let complete = async {
        store.wait_until_blocked().await;
        let result = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &inside)
            .engine()
            .complete_upload(
                &upload_id,
                loonfs_types::CompleteUploadBody::ServiceProxied {},
            )
            .await;
        store.release();
        result
    };
    let (report, completion) = tokio::join!(gc, complete);
    completion.expect("completion wins the blocked abort CAS");
    let report = report.expect("collection");
    assert_eq!(report.deleted.upload_sessions, 0);
    let session = read_upload_session(&store, &pool_id, &upload_id)
        .await
        .expect("completed session retained");
    assert_matches!(session.outcome, Some(UploadOutcome::Completed { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "the losing abort must not clean up the winner's content"
    );
}

#[tokio::test]
async fn gc_abort_wins_before_completion_and_completion_reports_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let aged = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GC_GRACE_WINDOW_MS + 1);
    let content_key = loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id);
    let store = blocking_outcome_store(store, BlockedOutcome::Completed);
    let engine =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &inside).engine();
    let completion = engine.complete_upload(
        &upload_id,
        loonfs_types::CompleteUploadBody::ServiceProxied {},
    );
    let abort = async {
        store.wait_until_blocked().await;
        let report = crate::test_support::pool_store(&store, &pool_id)
            .gc(None, &aged.clock())
            .await;
        store.release();
        report
    };
    let (completion, report) = tokio::join!(completion, abort);
    let error = completion.expect_err("an aborted session is logically absent");
    assert_matches!(&error, CoreError::UploadNotFound { .. });
    assert_eq!(error.code(), crate::error::ErrorCode::UploadNotFound);
    report.expect("collection");
    let session = read_upload_session(&store, &pool_id, &upload_id)
        .await
        .expect("aborted session retained for a grace window");
    assert_matches!(session.outcome, Some(UploadOutcome::Aborted { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "the winning abort cleans up, and the losing completion does not resurrect"
    );
}
