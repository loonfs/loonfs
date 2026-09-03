//! Namespace writer session lifecycle and admission policy.

#![allow(clippy::panic)]

use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, ErrorCode, FsWriter, NamespaceId,
    NamespaceSessionPolicy, NamespaceSessionState, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

async fn writer(store: SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer")
}

fn directory_options() -> CreateDirectoryOptions {
    CreateDirectoryOptions::new(loonfs_test_support::test_actor())
}

async fn create_namespace(writer: &FsWriter, namespace_id: &NamespaceId) {
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
}

async fn writer_epoch(store: &SharedObjectStore, namespace_id: &NamespaceId) -> u64 {
    loonfs::control::load_namespace_head_control(store, namespace_id)
        .await
        .expect("load namespace head")
        .state
        .writer_epoch
        .0
}

fn expect_code<T: std::fmt::Debug>(result: loonfs::Result<T>, code: ErrorCode) {
    let error = result.expect_err("operation must fail");
    assert_eq!(error.code(), code, "unexpected error: {error:?}");
}

#[tokio::test]
async fn concurrent_opens_create_one_namespace_session() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = writer(store, "concurrent-open").await;
    let namespace_id = NamespaceId::parse("concurrent").expect("namespace id");
    let barrier = Arc::new(Barrier::new(8));

    std::thread::scope(|scope| {
        for _ in 0..8 {
            let writer = writer.clone();
            let namespace_id = namespace_id.clone();
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                writer
                    .open_namespace(&namespace_id)
                    .expect("open namespace session");
            });
        }
    });

    assert_eq!(writer.writer_session_stats().open, 1);
    assert_eq!(
        writer.namespace_session_state(&namespace_id),
        NamespaceSessionState::Open {
            fenced: false,
            queued_commits: 0,
        }
    );
}

#[tokio::test]
async fn close_refuses_late_work_and_drains_admitted_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("close-drain").expect("namespace id");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create store"),
        KeyPredicate::wal_head(&namespace_id),
        OperationClass::CompareAndSwap,
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = writer(store, "close-drain").await;
    create_namespace(&writer, &namespace_id).await;

    blocking.block_next();
    let first = tokio::spawn({
        let writer = writer.clone();
        let namespace_id = namespace_id.clone();
        async move {
            writer
                .create_directory(&namespace_id, "/first", directory_options())
                .await
        }
    });
    blocking.wait_until_blocked().await;
    let open_before_close = writer.writer_session_stats().open;

    let mut second =
        Box::pin(writer.create_directory(&namespace_id, "/second", directory_options()));
    assert!(futures::poll!(second.as_mut()).is_pending());
    let mut close = Box::pin(writer.close_namespace(&namespace_id));
    assert!(futures::poll!(close.as_mut()).is_pending());
    assert_eq!(
        writer.namespace_session_state(&namespace_id),
        NamespaceSessionState::Closing
    );
    drop(close);
    let joined_close = tokio::spawn({
        let writer = writer.clone();
        let namespace_id = namespace_id.clone();
        async move { writer.close_namespace(&namespace_id).await }
    });

    expect_code(
        writer
            .create_directory(&namespace_id, "/third", directory_options())
            .await,
        ErrorCode::WriterSessionClosed,
    );

    blocking.release();
    first
        .await
        .expect("join first mutation")
        .expect("first mutation lands");
    second.await.expect("second mutation lands");
    let report = joined_close
        .await
        .expect("join close waiter")
        .expect("wait for namespace close");
    assert!(!report.was_open);
    assert_eq!(report.drained_commits, 0);
    assert!(!report.fenced);
    assert_eq!(
        writer.namespace_session_state(&namespace_id),
        NamespaceSessionState::Closed
    );
    writer
        .create_directory(&namespace_id, "/after", directory_options())
        .await
        .expect("a later mutation opens a new session");
    assert_eq!(writer.writer_session_stats().open, open_before_close);
}

#[tokio::test]
async fn closing_session_holds_capacity_and_shutdown_waits_for_it() {
    let temp_dir = tempdir().expect("tempdir");
    let first = NamespaceId::parse("closing-capacity-one").expect("namespace id");
    let second = NamespaceId::parse("closing-capacity-two").expect("namespace id");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create store"),
        KeyPredicate::wal_head(&first),
        OperationClass::CompareAndSwap,
    ));
    let store: SharedObjectStore = blocking.clone();
    let setup = writer(store.clone(), "closing-capacity-setup").await;
    create_namespace(&setup, &first).await;
    create_namespace(&setup, &second).await;
    setup.shutdown().await.expect("shut down setup writer");

    let writer = FsWriter::builder_with_store(store)
        .writer_id("closing-capacity")
        .min_publish_interval_ms(0)
        .max_open_namespaces(NonZeroUsize::new(1).expect("nonzero capacity"))
        .build()
        .await
        .expect("build bounded writer");
    blocking.block_next();
    let mutation = tokio::spawn({
        let writer = writer.clone();
        let first = first.clone();
        async move {
            writer
                .create_directory(&first, "/first", directory_options())
                .await
        }
    });
    blocking.wait_until_blocked().await;

    let mut close = Box::pin(writer.close_namespace(&first));
    assert!(futures::poll!(close.as_mut()).is_pending());
    let stats = writer.writer_session_stats();
    assert_eq!(stats.open, 0);
    assert_eq!(stats.closing, 1);
    expect_code(
        writer.open_namespace(&second),
        ErrorCode::WriterCapacityExceeded,
    );

    let shutdown = tokio::spawn({
        let writer = writer.clone();
        async move { writer.shutdown().await }
    });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());

    blocking.release();
    mutation
        .await
        .expect("join mutation")
        .expect("mutation lands");
    let report = close.await.expect("close namespace session");
    assert!(report.was_open);
    assert_eq!(report.drained_commits, 1);
    shutdown
        .await
        .expect("join shutdown")
        .expect("shutdown waits for close");
}

#[tokio::test]
async fn reopening_starts_a_new_epoch_and_explicit_policy_requires_open() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("reopen").expect("namespace id");
    let automatic = writer(store.clone(), "automatic").await;
    create_namespace(&automatic, &namespace_id).await;
    automatic
        .create_directory(&namespace_id, "/before", directory_options())
        .await
        .expect("first session publishes");
    let before_close = writer_epoch(&store, &namespace_id).await;

    automatic
        .close_namespace(&namespace_id)
        .await
        .expect("close first session");
    automatic
        .create_directory(&namespace_id, "/after", directory_options())
        .await
        .expect("automatic policy reopens");
    assert_eq!(writer_epoch(&store, &namespace_id).await, before_close + 1);

    let explicit = FsWriter::builder_with_store(store)
        .writer_id("explicit")
        .min_publish_interval_ms(0)
        .namespace_sessions(NamespaceSessionPolicy::ExplicitOpen)
        .build()
        .await
        .expect("build explicit writer");
    expect_code(
        explicit
            .create_directory(&namespace_id, "/refused", directory_options())
            .await,
        ErrorCode::WriterSessionClosed,
    );
    explicit
        .open_namespace(&namespace_id)
        .expect("open assigned namespace");
    explicit
        .create_directory(&namespace_id, "/explicit", directory_options())
        .await
        .expect("explicitly opened session publishes");
}

#[tokio::test]
async fn fencing_is_sticky_until_the_session_is_closed() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("sticky-fence").expect("namespace id");
    let writer_a = writer(store.clone(), "writer-a").await;
    let writer_b = writer(store, "writer-b").await;
    create_namespace(&writer_a, &namespace_id).await;
    writer_a
        .create_directory(&namespace_id, "/a-one", directory_options())
        .await
        .expect("writer A acquires its session");
    writer_b
        .create_directory(&namespace_id, "/b-one", directory_options())
        .await
        .expect("writer B takes over");

    for path in ["/a-two", "/a-three"] {
        expect_code(
            writer_a
                .create_directory(&namespace_id, path, directory_options())
                .await,
            ErrorCode::WriterFenced,
        );
    }
    assert_eq!(
        writer_a.namespace_session_state(&namespace_id),
        NamespaceSessionState::Open {
            fenced: true,
            queued_commits: 0,
        }
    );
    assert_eq!(writer_a.writer_session_stats().fenced, 1);

    let report = writer_a
        .close_namespace(&namespace_id)
        .await
        .expect("close fenced session");
    assert!(report.fenced);
    writer_a
        .create_directory(&namespace_id, "/a-four", directory_options())
        .await
        .expect("writer A opens a new session");
    expect_code(
        writer_b
            .create_directory(&namespace_id, "/b-two", directory_options())
            .await,
        ErrorCode::WriterFenced,
    );
}

#[tokio::test]
async fn capacity_refuses_without_eviction_and_close_releases_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let writer = FsWriter::builder_with_store(store)
        .writer_id("bounded")
        .min_publish_interval_ms(0)
        .max_open_namespaces(NonZeroUsize::new(2).expect("nonzero capacity"))
        .build()
        .await
        .expect("build bounded writer");
    let first = NamespaceId::parse("capacity-one").expect("namespace id");
    let second = NamespaceId::parse("capacity-two").expect("namespace id");
    let third = NamespaceId::parse("capacity-three").expect("namespace id");
    for namespace_id in [&first, &second, &third] {
        create_namespace(&writer, namespace_id).await;
    }
    writer.open_namespace(&first).expect("open first session");
    writer.open_namespace(&second).expect("open second session");

    expect_code(
        writer
            .create_directory(&third, "/refused", directory_options())
            .await,
        ErrorCode::WriterCapacityExceeded,
    );
    writer
        .create_directory(&first, "/first", directory_options())
        .await
        .expect("first session remains open");
    writer
        .create_directory(&second, "/second", directory_options())
        .await
        .expect("second session remains open");
    writer
        .close_namespace(&first)
        .await
        .expect("close first session");
    writer
        .create_directory(&third, "/third", directory_options())
        .await
        .expect("third session opens after close");
    writer
        .close_namespace(&second)
        .await
        .expect("close second session");
    writer
        .close_namespace(&third)
        .await
        .expect("close third session");

    for index in 0..20 {
        let namespace_id =
            NamespaceId::parse(format!("cycled-{index}")).expect("valid cycled namespace id");
        writer
            .open_namespace(&namespace_id)
            .expect("open cycled session");
        create_namespace(&writer, &namespace_id).await;
        writer
            .create_directory(&namespace_id, "/written", directory_options())
            .await
            .expect("write through cycled session");
        writer
            .close_namespace(&namespace_id)
            .await
            .expect("close cycled session");
    }
    assert_eq!(writer.writer_session_stats().open, 0);
}
