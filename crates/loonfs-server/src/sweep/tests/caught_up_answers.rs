//! The caught-up answer that a held session keeps after it reads its tail
//! from the store, and what makes a tick read the tail again.

use super::{idle_fold_after_ms, seed_writer, sweep_config, SweepServer, DAY_MS};
use loonfs::{ErrorCode, SharedObjectStore};
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PutMode;
use loonfs_test_support::clock::bounded;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, OperationKind};
use loonfs_test_support::test_actor;
use loonfs_types::limits::READ_REVALIDATION_BOUND_MS;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Checks that a batch whose WAL put lands with an unknown outcome voids the
/// session's caught-up answer, though the published seq doesn't move, so a
/// tick reads the tail that the batch left and folds it at its idle fold age.
///
/// The commit's WAL put is written, its response is lost, and the reads that
/// would resolve the outcome fail until the commit returns.
#[tokio::test]
async fn a_tail_that_a_commit_with_an_unknown_outcome_left_is_folded() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("unknown-outcome");
    let wal_prefix = loonfs_objectstore::keys::wal_prefix(&namespace_id);
    let armed = Arc::new(AtomicBool::new(false));
    let lost = Arc::new(AtomicBool::new(false));
    let lost_ack = FailStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        {
            let (armed, lost) = (Arc::clone(&armed), Arc::clone(&lost));
            move |operation| {
                operation.key().starts_with(&wal_prefix)
                    && matches!(
                        operation.kind(),
                        OperationKind::Put {
                            mode: PutMode::CreateIfAbsent,
                            ..
                        }
                    )
                    && armed.swap(false, Ordering::SeqCst)
                    && !lost.swap(true, Ordering::SeqCst)
            }
        },
        InjectedError::Transport("lost WAL acknowledgement".to_owned()),
    )
    .apply_then_fail();
    lost_ack.fail_all();
    let store = Arc::new(FailStore::matching(
        lost_ack,
        {
            let lost = Arc::clone(&lost);
            move |operation| {
                lost.load(Ordering::SeqCst)
                    && matches!(
                        operation.kind(),
                        OperationKind::Get { .. }
                            | OperationKind::GetWithMetadata
                            | OperationKind::Head
                    )
            }
        },
        InjectedError::Transport("recovery reads unavailable".to_owned()),
    ));
    store.fail_all();
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    held.put_file("/first", b"data", &test_actor())
        .await
        .expect("publish");
    // Past the revalidation bound, the tick reads the tail from the store,
    // folds it, and keeps the caught-up answer.
    server.tick_after(READ_REVALIDATION_BOUND_MS).await;
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);

    armed.store(true, Ordering::SeqCst);
    let error = bounded(
        Duration::from_secs(10),
        held.put_file("/landed", b"data", &test_actor()),
    )
    .await
    .expect("the commit returns")
    .expect_err("the commit's outcome is unknown");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown, "{error:?}");
    lost.store(false, Ordering::SeqCst);
    assert!(
        server.wal_tail_objects(&namespace_id).await > 0,
        "the commit's WAL object was written"
    );

    server.tick_after(idle_fold_after_ms(&config)).await;
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "the attempted batch voids the kept answer, so the tick reads the tail and folds it"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a tail that another writer left on a namespace that this
/// host holds a session for is folded by a tick once the session's kept
/// answer reaches the revalidation bound, without a daily pass. The
/// session never published, so it has no position.
#[tokio::test]
async fn a_tick_folds_another_writers_tail_once_the_kept_answer_expires() {
    let directory = tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let (writer, now_ms) = seed_writer(&store).await;
    let namespace_id = namespace_id("written-elsewhere");
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let config = sweep_config(None);
    let server = SweepServer::start(store, &config, now_ms + DAY_MS).await;
    drop(
        server
            .sweep
            .inner
            .namespaces
            .open(&namespace_id)
            .await
            .expect("open"),
    );
    server.tick_after(config.tick_interval_ms).await;

    writer
        .open_namespace(&namespace_id)
        .expect("open the other writer's session")
        .put_file("/elsewhere", b"data", &test_actor())
        .await
        .expect("the other writer commits");
    writer.shutdown().await.expect("stop the other writer");
    server.tick_after(config.tick_interval_ms).await;
    assert!(
        server.wal_tail_objects(&namespace_id).await > 0,
        "the session's kept answer still applies"
    );
    server.tick_after(READ_REVALIDATION_BOUND_MS).await;
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "the tick reads the tail again and folds it"
    );
    server.runtime.shutdown().await.expect("shutdown");
}
