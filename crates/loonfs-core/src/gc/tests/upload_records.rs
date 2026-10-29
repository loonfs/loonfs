//! Races between garbage collection and the two records of an upload
//! session: a pass that stops between its two deletes, and a completion or
//! abort that stalls while a pass deletes the session.

use super::*;
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::keys::{
    content_blob, upload_outcome, upload_session, upload_session_prefix,
};
use loonfs_types::api::v0::CompleteUploadBody;
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
        namespace_id.clone(),
        Arc::new(crate::test_support::content_writes()),
    )
    .publish_batch(
        store,
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
        &context(2_000),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
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
) -> crate::error::Result<crate::protocol::CompletedUpload> {
    crate::protocol::complete_upload(
        store,
        namespace_id,
        upload_id,
        None,
        CompleteUploadBody::ServiceProxied {},
        &context(now_ms),
    )
    .await
}

async fn abort<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    now_ms: u64,
) -> crate::error::Result<loonfs_types::api::v0::UploadSession> {
    let catalog = crate::namespace::catalog::load_namespace_catalog_entry(store, namespace_id)
        .await
        .expect("catalog");
    crate::protocol::abort_upload(store, &catalog, upload_id, None, &context(now_ms)).await
}

async fn upload_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    store
        .list_prefix(&upload_session_prefix(namespace_id))
        .await
        .expect("list upload records")
}

async fn pass<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> crate::error::Result<GcResponse> {
    gc_namespace(
        store,
        None,
        None,
        namespace_id,
        &options(),
        &context(now_ms),
        crate::test_support::std_timer(),
    )
    .await
}

/// A pass that stops between deleting a session's two records leaves the
/// outcome record without its session record. The next pass deletes that
/// outcome and keeps the committed content. If the outcome record were
/// deleted first, the session record left behind would read as an open,
/// expired session, and the next pass would abort it and delete the content.
#[tokio::test]
async fn a_pass_that_stops_between_the_two_deletes_leaves_an_outcome_the_next_pass_removes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let prefix = upload_session_prefix(&namespace_id);
    let upload_deletes = Arc::new(AtomicUsize::new(0));
    // Fails the second delete of an upload record, whichever record it is.
    let store = FailStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        {
            let upload_deletes = Arc::clone(&upload_deletes);
            move |operation: &OperationContext<'_>| {
                operation.key().starts_with(&prefix)
                    && matches!(operation.kind(), OperationKind::Delete)
                    && upload_deletes.fetch_add(1, Ordering::SeqCst) == 1
            }
        },
        InjectedError::Transport("the process stopped".to_owned()),
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let completed = complete_proxied(&store, &namespace_id, &upload_id, 2_000)
        .await
        .expect("complete");
    commit_upload(&store, &namespace_id, "/committed.txt", completed.prepared).await;
    let content_key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);
    let past_grace = now_after_newest_object(
        &store,
        &namespace_id,
        CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1,
    )
    .await;

    upload_deletes.store(0, Ordering::SeqCst);
    store.fail_next(1);
    pass(&store, &namespace_id, past_grace)
        .await
        .expect_err("the pass stops at its second delete");
    assert_eq!(
        upload_keys(&store, &namespace_id).await,
        [upload_outcome(&namespace_id, &upload_id)],
        "the session record goes first"
    );
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());

    let report = pass(&store, &namespace_id, past_grace)
        .await
        .expect("the next pass");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(report.deleted.content_objects, 0);
    assert!(upload_keys(&store, &namespace_id).await.is_empty());
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "the committed content stays"
    );
    let report = pass(&store, &namespace_id, past_grace)
        .await
        .expect("a later pass");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());
}

/// A completion that read the session as open, and then stalled while the
/// session was aborted and a pass deleted its records, creates an outcome
/// record for a session that no longer exists. The completion finds the
/// session record gone and refuses, so it returns no token, and the next
/// pass deletes the outcome it left.
#[tokio::test]
async fn a_completion_stalled_across_a_pass_that_deleted_an_aborted_session_is_refused() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let store = blocking_outcome_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        BlockedOutcome::Completed,
    );
    create(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let content_key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);
    let aborted_at_ms = 3_000;
    let past_abort_grace = aborted_at_ms + GRACE_MS + 1;

    let (completion, ()) = tokio::join!(
        complete_proxied(&store, &namespace_id, &upload_id, 2_000),
        async {
            store.wait_until_blocked().await;
            abort(store.inner(), &namespace_id, &upload_id, aborted_at_ms)
                .await
                .expect("abort while the completion is stalled");
            let report = pass(store.inner(), &namespace_id, past_abort_grace)
                .await
                .expect("a pass deletes the aborted session");
            assert_eq!(report.deleted.upload_sessions, 1);
            assert!(upload_keys(store.inner(), &namespace_id).await.is_empty());
            store.release();
        }
    );

    assert_matches!(completion, Err(CoreError::UploadNotFound { .. }));
    assert_eq!(
        upload_keys(&store, &namespace_id).await,
        [upload_outcome(&namespace_id, &upload_id)],
        "the stalled completion's outcome is left without its session"
    );
    for result in [
        complete_proxied(&store, &namespace_id, &upload_id, 4_000)
            .await
            .map(drop),
        crate::protocol::get_upload(&store, &namespace_id, &upload_id, None, 4_000)
            .await
            .map(drop),
    ] {
        assert_matches!(result, Err(CoreError::UploadNotFound { .. }));
    }
    // The leftover outcome is deleted once it's older than the grace window.
    let outcome_aged = now_after_newest_object(&store, &namespace_id, GRACE_MS).await;
    let report = pass(&store, &namespace_id, outcome_aged)
        .await
        .expect("the next pass");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(upload_keys(&store, &namespace_id).await.is_empty());
    assert!(store.head(&content_key).await.expect("head").is_none());
}

/// An outcome record without its session record is deleted only once it's
/// at least the grace window old by its listing time. A pass can't tell a
/// leftover outcome from the new outcome of a session whose session record
/// its listing missed, so a young one survives.
#[tokio::test]
async fn an_outcome_without_its_session_record_is_deleted_only_once_older_than_the_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("bootstrap");
    let session = crate::test_support::proxied_upload_session(&namespace_id, 2_000);
    let session_key = crate::test_support::write_upload_records(
        &store,
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
    let outcome_key = upload_outcome(&namespace_id, &session.upload_id);
    let written_at_ms = store
        .head(&outcome_key)
        .await
        .expect("head")
        .expect("outcome")
        .last_modified_at_ms
        .expect("the local store reports modification times");

    let young = pass(&store, &namespace_id, written_at_ms + GRACE_MS - 1)
        .await
        .expect("a pass inside the grace window");
    assert_eq!(young.deleted.upload_sessions, 0);
    assert_eq!(upload_keys(&store, &namespace_id).await, [outcome_key]);

    let aged = pass(&store, &namespace_id, written_at_ms + GRACE_MS)
        .await
        .expect("a pass at the grace window");
    assert_eq!(aged.deleted.upload_sessions, 1);
    assert!(upload_keys(&store, &namespace_id).await.is_empty());
}

/// An abort that read the session as open, and then stalled while the
/// session completed, its content was committed, and a pass deleted the
/// session's records after the content grace, would otherwise delete
/// committed content. The abort finds the session record gone and deletes
/// nothing.
#[tokio::test]
async fn an_abort_stalled_across_a_pass_that_deleted_a_completed_session_keeps_its_content() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let store = blocking_outcome_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        BlockedOutcome::Aborted,
    );
    create(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let content_key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);

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
        let report = pass(store.inner(), &namespace_id, past_grace)
            .await
            .expect("a pass deletes the completed session");
        assert_eq!(report.deleted.upload_sessions, 1);
        store.release();
    });

    assert_matches!(aborted, Err(CoreError::UploadNotFound { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "the stalled abort deletes no committed content"
    );
    assert!(store
        .head(&upload_session(&namespace_id, &upload_id))
        .await
        .expect("head")
        .is_none());
}
