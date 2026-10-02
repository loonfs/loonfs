//! Admission and task ownership after a writer session is fenced.

use super::*;
use crate::{ExecutionBudgetStats, LoonFs, Namespace, Writable};

async fn superseded_session(store: SharedStore) -> (LoonFs<Writable>, Namespace<Writable>) {
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .min_publish_interval_ms(0)
        .execution_budget(
            ExecutionBudget::builder()
                .max_concurrent_publications(NonZeroUsize::MIN)
                .build(),
        )
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("fenced").expect("namespace id");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    namespace
        .create_directory("/first", &actor)
        .await
        .expect("first writer");
    writer.drain().await.expect("first publication ends");
    let rival = LoonFs::builder_with_store(store)
        .writer_id("writer-b")
        .build()
        .await
        .expect("rival");
    rival
        .open_namespace(&namespace_id)
        .expect("rival session")
        .create_directory("/second", &actor)
        .await
        .expect("take over");
    rival.shutdown().await.expect("rival shutdown");
    (writer, namespace)
}

async fn discover_fence(namespace: &Namespace<Writable>) -> Error {
    let error = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "discover", "discover",
        )))
        .await
        .expect_err("discover the fence");
    assert_eq!(error.code(), ErrorCode::WriterFenced);
    assert_eq!(namespace.session_state(), NamespaceSessionState::Fenced);
    error
}

fn assert_no_work(publisher: &NamespacePublisher) {
    let state = publisher.lock_state();
    assert!(state.queue.is_empty());
    assert!(state.in_flight.is_empty());
    assert!(state.worker.is_none());
    assert!(state.fold.is_none());
    assert!(state.compaction.is_none());
    assert_eq!(state.next_task_id, 0);
}

async fn assert_old_handle_refuses_submission(delete: bool) {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let (writer, old) = superseded_session(store.clone()).await;
    let first_error = discover_fence(&old).await;
    writer.drain().await.expect("fenced work ends");
    let publisher = &old.session().publisher;
    assert!(writer.mode.publisher.live_publisher(old.id()).is_none());
    let replacement = writer.open_namespace(old.id()).expect("replacement");
    assert!(!Arc::ptr_eq(
        &publisher.state,
        &replacement.session().publisher.state
    ));
    let capacity = writer.execution_budget().publication_permit().await;
    let before = writer.execution_budget().stats();
    let usage = publisher.admission.namespace_usage(old.id());
    let requests = store.counts();
    let mut submission = Box::pin(async {
        if delete {
            old.delete().await.map(|_| ())
        } else {
            old.commit_candidate(CommitCandidate::new(create_directory_request(
                "late", "late",
            )))
            .await
            .map(|_| ())
        }
    });
    let error = match futures::poll!(submission.as_mut()) {
        std::task::Poll::Ready(Err(error)) => error,
        other => panic!("expected an immediate fencing error, got {other:?}"),
    };
    assert_eq!(error.to_api_error(), first_error.to_api_error());
    assert_eq!(publisher.admission.namespace_usage(old.id()), usage);
    assert_eq!(writer.execution_budget().stats(), before);
    assert_eq!(store.counts(), requests);
    assert_no_work(publisher);
    timeout(Duration::from_secs(10), writer.shutdown())
        .await
        .expect("shutdown completes")
        .expect("shutdown");
    assert_no_work(publisher);
    assert_no_work(&replacement.session().publisher);
    assert_eq!(writer.execution_budget().stats(), before);
    drop(capacity);
    assert_eq!(
        writer.execution_budget().stats(),
        ExecutionBudgetStats::default()
    );
}

#[tokio::test]
async fn an_old_fenced_handle_refuses_a_commit_before_admission() {
    assert_old_handle_refuses_submission(false).await;
}

#[tokio::test]
async fn an_old_fenced_handle_refuses_a_delete_before_admission() {
    assert_old_handle_refuses_submission(true).await;
}

#[tokio::test]
async fn a_fenced_session_starts_no_fold_or_compaction() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let (writer, old) = superseded_session(store.clone()).await;
    discover_fence(&old).await;
    writer.drain().await.expect("fenced work ends");
    let replacement = writer.open_namespace(old.id()).expect("replacement");
    let publisher = &old.session().publisher;
    let requests = store.counts();
    assert!(publisher.start_fold().is_none());
    publisher.start_compaction();
    assert_no_work(publisher);
    assert_eq!(store.counts(), requests);
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    assert_eq!(
        maintenance
            .fold_wal(old.id())
            .await
            .expect("explicit fold")
            .outcome,
        FoldWalOutcome::Published
    );
    assert_no_work(publisher);
    assert_no_work(&replacement.session().publisher);
    let requests = store.counts();
    let report = old.clone().close().await.expect("close old session");
    assert!(report.fenced);
    assert_no_work(publisher);
    assert_eq!(store.counts(), requests);
    assert_eq!(replacement.session_state(), NamespaceSessionState::Open);
    writer.shutdown().await.expect("shutdown");
    assert_eq!(
        writer.execution_budget().stats(),
        ExecutionBudgetStats::default()
    );
}

#[tokio::test]
async fn a_candidate_charged_before_fencing_is_refused_at_queue_insertion() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let (writer, old) = superseded_session(store).await;
    let publisher = &old.session().publisher;
    let candidate = PreparedCandidate::new(CommitCandidate::new(create_directory_request(
        "late", "late",
    )))
    .expect("candidate");
    let identity = candidate
        .candidate
        .semantic_identity(old.id())
        .expect("identity");
    let permit = publisher
        .admission
        .acquire_candidate(old.id(), &candidate)
        .expect("admit before fencing");
    assert_eq!(permit.reserve_inline([4].into_iter(), 0, 4), 1);
    discover_fence(&old).await;
    writer.drain().await.expect("fenced work ends");
    let _replacement = writer.open_namespace(old.id()).expect("replacement");
    let (sender, _receiver) = oneshot::channel();
    let admission = publisher.admit(
        candidate.candidate.commit_id().clone(),
        candidate,
        identity,
        AdmittedWaiter::new(sender, &permit),
        0,
    );
    assert!(matches!(admission, Err(CoreError::WriterFenced(_))));
    drop(permit);
    assert_no_work(publisher);
    assert_eq!(publisher.admission.namespace_usage(old.id()), (0, 0, 0));
    assert_eq!(
        writer.execution_budget().stats(),
        ExecutionBudgetStats::default()
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn queued_work_keeps_a_fenced_session_registered_until_it_settles() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let (writer, old) = superseded_session(store.clone()).await;
    let publisher = &old.session().publisher;
    let engine = publisher.engine.lock().await;
    let mut discovery = Box::pin(discover_fence(&old));
    assert!(futures::poll!(discovery.as_mut()).is_pending());
    timeout(Duration::from_secs(10), async {
        while writer.execution_budget().stats().publications_running != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker holds publication capacity");
    let mut queued_commit = Box::pin(old.commit_candidate(CommitCandidate::new(
        create_directory_request("queued", "queued"),
    )));
    let mut queued_delete = Box::pin(
        old.session()
            .submit_delete(DeleteNamespaceOptions::default()),
    );
    assert!(futures::poll!(queued_commit.as_mut()).is_pending());
    assert!(futures::poll!(queued_delete.as_mut()).is_pending());
    assert_eq!(publisher.lock_state().queue.len(), 2);
    let mut capacity = Box::pin(writer.execution_budget().publication_permit());
    assert!(futures::poll!(capacity.as_mut()).is_pending());
    drop(engine);
    let first_error = discovery.await;
    let capacity = capacity.await;
    assert!(writer.mode.publisher.live_publisher(old.id()).is_some());
    assert_eq!(
        writer
            .open_namespace(old.id())
            .expect_err("queued work still belongs to old session")
            .code(),
        ErrorCode::WriterSessionClosed
    );
    writer
        .maintenance(loonfs_test_support::ids::writer_id("maintenance"))
        .fold_wal(old.id())
        .await
        .expect("explicit fold records its outcome in the fenced session");
    {
        let state = publisher.lock_state();
        assert_eq!(state.next_task_id, 0);
        assert!(state.fold.is_none());
        assert!(state.compaction.is_none());
        assert_eq!(state.queue.len(), 2);
    }
    let requests = store.counts();
    let mut shutdown = Box::pin(writer.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    drop(capacity);
    assert_eq!(
        queued_commit
            .await
            .expect_err("queued commit is fenced")
            .to_api_error(),
        first_error.to_api_error()
    );
    assert_eq!(
        queued_delete
            .await
            .expect_err("queued delete is fenced")
            .to_api_error(),
        first_error.to_api_error()
    );
    timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("shutdown drains queued work")
        .expect("shutdown");
    assert_no_work(publisher);
    assert!(writer.mode.publisher.live_publisher(old.id()).is_none());
    assert_eq!(store.counts(), requests);
    assert_eq!(publisher.admission.namespace_usage(old.id()), (0, 0, 0));
    assert_eq!(
        writer.execution_budget().stats(),
        ExecutionBudgetStats::default()
    );
}
