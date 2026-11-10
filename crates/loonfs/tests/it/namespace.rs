//! The per-namespace handle: opening, sharing, closing, dropping, epochs,
//! and the read-only mode.

use crate::common::{collect_checkpoints, data_wal_put_for, expect_code, writer_epoch};
use loonfs::test_support::writer;
use loonfs::{
    CreateNamespaceOptions, CreationId, DeleteNamespaceOptions, DeletionId, ErrorCode,
    ForkNamespaceOptions, LoonFs, NamespaceId, NamespaceSessionStatus, RuntimeConfig,
    SharedObjectStore, Writable,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, RecordingStore};
use std::num::NonZeroU64;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

async fn create_namespace(writer: &LoonFs<Writable>, namespace_id: &NamespaceId) {
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
}

#[tokio::test]
async fn a_namespace_created_without_a_window_has_the_runtimes_default_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let default_window_ms = NonZeroU64::new(30 * 86_400_000);
    let own_window_ms = NonZeroU64::new(86_400_000);
    let writer = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("default-window"))
        .config(RuntimeConfig {
            default_retention_window_ms: default_window_ms,
            ..RuntimeConfig::default()
        })
        .build()
        .await
        .expect("build writer");
    for (name, requested, expected) in [
        ("defaulted", None, default_window_ms),
        ("own", own_window_ms, own_window_ms),
    ] {
        let namespace_id = NamespaceId::parse(name).expect("namespace id");
        writer
            .create_namespace_with_options(
                &namespace_id,
                &loonfs_test_support::test_actor(),
                &CreateNamespaceOptions {
                    retention_window_ms: requested,
                    ..CreateNamespaceOptions::default()
                },
            )
            .await
            .expect("create namespace");
        let metadata = writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .metadata()
            .await
            .expect("namespace metadata");
        assert_eq!(
            metadata.retention_window_ms,
            expected.map(NonZeroU64::get),
            "{name}"
        );
    }
}

/// Checks that a fork and a delete sent again with their IDs return their
/// first results through the runtime. The retried fork finds its target
/// before it creates a fork pin, and the retried delete finds the tombstone
/// through the deleting handle's session and through a new handle.
#[tokio::test]
async fn a_retried_fork_and_a_retried_delete_return_their_first_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = writer(store, "retries").await;
    let actor = loonfs_test_support::test_actor();
    let source = NamespaceId::parse("retried-source").expect("namespace id");
    let target = NamespaceId::parse("retried-fork").expect("namespace id");
    create_namespace(&writer, &source).await;
    let fork = |creation_id: &str| ForkNamespaceOptions {
        creation_id: Some(CreationId::parse(creation_id).expect("creation ID")),
        ..ForkNamespaceOptions::default()
    };
    let first = writer
        .fork_namespace_with_options(&source, &target, &actor, &fork("job-1-fork"))
        .await
        .expect("fork");
    let retried = writer
        .fork_namespace_with_options(&source, &target, &actor, &fork("job-1-fork"))
        .await
        .expect("a retried fork returns the fork");
    assert_eq!(retried, first);
    expect_code(
        writer
            .fork_namespace_with_options(&source, &target, &actor, &fork("job-2-fork"))
            .await,
        ErrorCode::NamespaceExists,
    );
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("retries-gc"));
    let checkpoints = collect_checkpoints(&maintenance, &source)
        .await
        .expect("list the source's pins");
    assert_eq!(checkpoints.checkpoints.len(), 1, "one fork pin");

    let delete = |deletion_id: &str| DeleteNamespaceOptions {
        deletion_id: Some(DeletionId::parse(deletion_id).expect("deletion ID")),
        ..DeleteNamespaceOptions::default()
    };
    let handle = writer.open_namespace(&target).expect("open the fork");
    let deleted = handle
        .delete_with_options(&delete("job-1-delete"))
        .await
        .expect("delete");
    assert_eq!(
        handle
            .delete_with_options(&delete("job-1-delete"))
            .await
            .expect("a retry through the deleting handle"),
        deleted
    );
    let reopened = writer
        .open_namespace(&target)
        .expect("open the deleted fork");
    assert_eq!(
        reopened
            .delete_with_options(&delete("job-1-delete"))
            .await
            .expect("a retry through a new handle"),
        deleted
    );
    expect_code(
        reopened.delete_with_options(&delete("job-2-delete")).await,
        ErrorCode::NamespaceDeleted,
    );
}

#[tokio::test]
async fn opening_does_no_store_io_and_the_first_publish_acquires_the_epoch() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = writer(store.clone(), "cheap-open").await;
    let namespace_id = NamespaceId::parse("cheap-open").expect("namespace id");
    create_namespace(&writer, &namespace_id).await;
    let created = writer_epoch(&store, &namespace_id).await;

    recording.reset();
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let operations = recording.take();
    assert!(operations.is_empty(), "opening touched {operations:?}");
    assert_eq!(writer_epoch(&store, &namespace_id).await, created);

    namespace
        .create_directory("/first", &loonfs_test_support::test_actor())
        .await
        .expect("first publish");
    assert_eq!(writer_epoch(&store, &namespace_id).await, created + 1);
}

#[tokio::test]
async fn concurrent_opens_share_one_session_and_one_epoch() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = writer(store.clone(), "shared-open").await;
    let namespace_id = NamespaceId::parse("shared-open").expect("namespace id");
    create_namespace(&writer, &namespace_id).await;
    let created = writer_epoch(&store, &namespace_id).await;
    let barrier = Arc::new(Barrier::new(8));

    let namespaces = std::thread::scope(|scope| {
        let openers = (0..8)
            .map(|_| {
                let writer = writer.clone();
                let namespace_id = namespace_id.clone();
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    writer
                        .open_namespace(&namespace_id)
                        .expect("open namespace")
                })
            })
            .collect::<Vec<_>>();
        openers
            .into_iter()
            .map(|opener| opener.join().expect("join opener"))
            .collect::<Vec<_>>()
    });

    for (index, namespace) in namespaces.iter().enumerate() {
        namespace
            .create_directory(
                &format!("/from-{index}"),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("every handle publishes through the shared session");
    }
    assert_eq!(writer_epoch(&store, &namespace_id).await, created + 1);
}

#[tokio::test]
async fn close_drains_admitted_commits_and_refuses_later_work_from_every_clone() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("close-drain").expect("namespace id");
    let blocking = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("create store"),
        data_wal_put_for(&namespace_id),
    ));
    let writer = writer(blocking.clone(), "close-drain").await;
    create_namespace(&writer, &namespace_id).await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let clone = namespace.clone();

    blocking.block_next();
    let first = tokio::spawn({
        let namespace = namespace.clone();
        async move {
            namespace
                .create_directory("/first", &loonfs_test_support::test_actor())
                .await
        }
    });
    blocking.wait_until_blocked().await;
    let actor = loonfs_test_support::test_actor();
    let mut second = Box::pin(clone.create_directory("/second", &actor));
    assert!(futures::poll!(second.as_mut()).is_pending());

    let mut close = Box::pin(namespace.close());
    assert!(futures::poll!(close.as_mut()).is_pending());
    expect_code(
        clone
            .create_directory("/third", &loonfs_test_support::test_actor())
            .await,
        ErrorCode::WriterSessionClosed,
    );

    blocking.release();
    first
        .await
        .expect("join first mutation")
        .expect("first mutation commits");
    second.await.expect("second mutation commits");
    let report = close.await.expect("close namespace session");
    assert_eq!(report.drained_commits, Some(2));
    assert!(!report.fenced);
}

#[tokio::test]
async fn reopening_after_close_starts_a_new_epoch() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = writer(store.clone(), "reopen").await;
    let namespace_id = NamespaceId::parse("reopen").expect("namespace id");
    create_namespace(&writer, &namespace_id).await;

    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .create_directory("/before", &loonfs_test_support::test_actor())
        .await
        .expect("first session publishes");
    let first_epoch = writer_epoch(&store, &namespace_id).await;
    namespace.close().await.expect("close first session");

    let reopened = writer
        .open_namespace(&namespace_id)
        .expect("reopen namespace");
    reopened
        .create_directory("/after", &loonfs_test_support::test_actor())
        .await
        .expect("second session publishes");
    assert_eq!(writer_epoch(&store, &namespace_id).await, first_epoch + 1);
}

#[tokio::test]
async fn fencing_lasts_until_close_and_a_reopened_handle_takes_a_new_epoch() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("sticky-fence").expect("namespace id");
    let writer_a = writer(store.clone(), "writer-a").await;
    let writer_b = writer(store.clone(), "writer-b").await;
    create_namespace(&writer_a, &namespace_id).await;
    let handle_a = writer_a
        .open_namespace(&namespace_id)
        .expect("open writer A");
    let handle_b = writer_b
        .open_namespace(&namespace_id)
        .expect("open writer B");
    handle_a
        .create_directory("/a-one", &loonfs_test_support::test_actor())
        .await
        .expect("writer A acquires the epoch");
    handle_b
        .create_directory("/b-one", &loonfs_test_support::test_actor())
        .await
        .expect("writer B takes over");
    let taken_over = writer_epoch(&store, &namespace_id).await;

    for path in ["/a-two", "/a-three"] {
        expect_code(
            handle_a
                .create_directory(path, &loonfs_test_support::test_actor())
                .await,
            ErrorCode::WriterFenced,
        );
    }
    assert_eq!(handle_a.session_status(), NamespaceSessionStatus::Fenced);
    let report = handle_a.close().await.expect("close fenced session");
    assert!(report.fenced);

    let reopened_a = writer_a
        .open_namespace(&namespace_id)
        .expect("reopen writer A");
    reopened_a
        .create_directory("/a-four", &loonfs_test_support::test_actor())
        .await
        .expect("the reopened session acquires a new epoch");
    assert_eq!(writer_epoch(&store, &namespace_id).await, taken_over + 1);
}

#[tokio::test]
async fn dropping_the_last_clone_ends_the_session_and_a_kept_clone_keeps_it_open() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = writer(store.clone(), "drop-ends").await;
    let namespace_id = NamespaceId::parse("drop-ends").expect("namespace id");
    create_namespace(&writer, &namespace_id).await;

    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let kept = namespace.clone();
    namespace
        .create_directory("/first", &loonfs_test_support::test_actor())
        .await
        .expect("first session publishes");
    let first_epoch = writer_epoch(&store, &namespace_id).await;
    drop(namespace);

    let shared = writer
        .open_namespace(&namespace_id)
        .expect("open while a clone is kept");
    shared
        .create_directory("/shared", &loonfs_test_support::test_actor())
        .await
        .expect("the kept session publishes");
    assert_eq!(
        writer_epoch(&store, &namespace_id).await,
        first_epoch,
        "a kept clone keeps the session and its epoch"
    );

    drop(kept);
    drop(shared);
    writer
        .drain()
        .await
        .expect("let the dropped session's admitted work finish");
    let reopened = writer
        .open_namespace(&namespace_id)
        .expect("reopen namespace");
    assert_eq!(reopened.session_status(), NamespaceSessionStatus::Open);
    reopened
        .create_directory("/reopened", &loonfs_test_support::test_actor())
        .await
        .expect("the new session publishes");
    assert_eq!(
        writer_epoch(&store, &namespace_id).await,
        first_epoch + 1,
        "dropping the last clone ended the session"
    );
}

#[tokio::test]
async fn a_read_only_handle_does_no_io_and_does_not_keep_the_session_open() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = writer(store.clone(), "read-only").await;
    let namespace_id = NamespaceId::parse("read-only").expect("namespace id");
    create_namespace(&writer, &namespace_id).await;

    recording.reset();
    let namespace = writer.namespace(&namespace_id);
    let operations = recording.take();
    assert!(
        operations.is_empty(),
        "creating the handle touched {operations:?}"
    );

    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .create_directory("/first", &loonfs_test_support::test_actor())
        .await
        .expect("first session publishes");
    let first_epoch = writer_epoch(&store, &namespace_id).await;
    let read_only = namespace_writer.read_only();
    drop(namespace_writer);
    writer
        .drain()
        .await
        .expect("let the dropped session's admitted work finish");

    let reopened = writer
        .open_namespace(&namespace_id)
        .expect("reopen namespace");
    reopened
        .create_directory("/reopened", &loonfs_test_support::test_actor())
        .await
        .expect("the new session publishes");
    assert_eq!(
        writer_epoch(&store, &namespace_id).await,
        first_epoch + 1,
        "a read-only handle does not keep the session open"
    );
    for path in ["/first", "/reopened"] {
        read_only
            .stat(path)
            .await
            .expect("the derived handle reads both sessions' commits");
        namespace
            .stat(path)
            .await
            .expect("the reader's handle reads both sessions' commits");
    }
}

#[tokio::test]
async fn shutdown_publishes_the_commits_open_handles_queued_before_it_returns() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("shutdown-drain").expect("namespace id");
    let blocking = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("create store"),
        data_wal_put_for(&namespace_id),
    ));
    let writer = writer(blocking.clone(), "shutdown-drain").await;
    create_namespace(&writer, &namespace_id).await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    blocking.block_next();
    let first = tokio::spawn({
        let namespace = namespace.clone();
        async move {
            namespace
                .create_directory("/first", &loonfs_test_support::test_actor())
                .await
        }
    });
    blocking.wait_until_blocked().await;
    let actor = loonfs_test_support::test_actor();
    let mut queued = Box::pin(namespace.create_directory("/queued", &actor));
    assert!(futures::poll!(queued.as_mut()).is_pending());

    let mut shutdown = Box::pin(writer.shutdown());
    assert!(
        futures::poll!(shutdown.as_mut()).is_pending(),
        "the parked publish keeps shutdown waiting"
    );
    blocking.release();
    shutdown.await.expect("shut down the writer");

    match futures::poll!(queued.as_mut()) {
        std::task::Poll::Ready(result) => {
            result.expect("the queued commit publishes");
        }
        std::task::Poll::Pending => panic!("shutdown returned before the queued commit published"),
    }
    first
        .await
        .expect("join the parked commit")
        .expect("the parked commit publishes");
}

#[tokio::test]
async fn the_runtime_holds_as_many_sessions_as_the_host_opens() {
    // More than the 1,024 sessions the runtime once capped.
    const NAMESPACES: usize = 1_025;

    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = writer(store.clone(), "no-cap").await;
    let namespaces = (0..NAMESPACES)
        .map(|index| {
            let namespace_id =
                NamespaceId::parse(format!("no-cap-{index:04}")).expect("namespace id");
            writer
                .open_namespace(&namespace_id)
                .expect("every open succeeds")
        })
        .collect::<Vec<_>>();

    let first = namespaces[0].id().clone();
    create_namespace(&writer, &first).await;
    namespaces[0]
        .create_directory("/written", &loonfs_test_support::test_actor())
        .await
        .expect("the first session publishes");
    let epoch = writer_epoch(&store, &first).await;
    writer
        .open_namespace(&first)
        .expect("open the first namespace again")
        .create_directory("/again", &loonfs_test_support::test_actor())
        .await
        .expect("publish through the first session");
    assert_eq!(
        writer_epoch(&store, &first).await,
        epoch,
        "the first session is still the one open"
    );
    assert!(namespaces
        .iter()
        .all(|namespace| namespace.session_status() == NamespaceSessionStatus::Open));
}
