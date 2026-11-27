//! Embedded requests without networking or bearer credentials.

use crate::args::TransientRetry;
use crate::config::StoreConfig;
use crate::resolve::{Backend, ResolvedTarget};
use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_client::{NamespacePath, PayloadSource};
use loonfs_core::limits::FOLD_AT_WAL_OBJECTS;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;

#[test]
fn embedded_requests_need_no_socket_or_token_and_stream_past_the_server_body_limit() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime without a networking driver");
    runtime.block_on(async {
        let directory = tempfile::tempdir().expect("store directory");
        let mut target = ResolvedTarget::embedded(
            &StoreConfig::LocalFs(LocalFsStoreConfig {
                root: directory.path().display().to_string(),
                key_prefix: None,
            }),
            None,
            TransientRetry::Retry,
        )
        .await
        .expect("embedded profile without credentials");
        target.act_as(loonfs_test_support::test_actor());
        let path = NamespacePath::parse("demo", "/large.bin").expect("path");
        target
            .client
            .create_namespace(Some(path.namespace()))
            .await
            .expect("namespace without a listener");
        let chunk = Bytes::from(vec![42; 1024 * 1024]);
        let source = PayloadSource::stream(
            futures::stream::iter((0..257).map(move |_| Ok(chunk.clone()))).boxed(),
        );
        target
            .client
            .put_file_stream(&path, source)
            .await
            .expect("upload past 256 MiB");
        let mut stream = target
            .client
            .read_file_stream(&path)
            .await
            .expect("download past 256 MiB");
        let mut size_bytes = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("verified content");
            assert!(chunk.iter().all(|byte| *byte == 42));
            size_bytes += chunk.len();
        }
        assert_eq!(size_bytes, 257 * 1024 * 1024);
    });
}

/// Each command here runs on a Tokio runtime of its own that is dropped when
/// the command returns, the way the process exits after a real one.
#[test]
fn an_embedded_command_finishes_the_fold_it_started_before_returning() {
    let directory = tempfile::tempdir().expect("store directory");
    let config = StoreConfig::LocalFs(LocalFsStoreConfig {
        root: directory.path().display().to_string(),
        key_prefix: None,
    });
    let namespace_id = loonfs_types::NamespaceId::parse("demo").expect("namespace id");
    let open = || async {
        let mut target = ResolvedTarget::embedded(&config, None, TransientRetry::NoRetry)
            .await
            .expect("embedded profile");
        target.act_as(loonfs_test_support::test_actor());
        target
    };
    run_command(async {
        let target = open().await;
        target
            .client
            .create_namespace(Some(&namespace_id))
            .await
            .expect("namespace");
        let store = embedded_host(&target).runtime.object_store();
        loonfs_core::test_support::NamespaceFixture::new(
            store.as_ref(),
            &namespace_id,
            &loonfs_core::test_support::WriterAt {
                writer_id: loonfs_types::WriterId::parse("tail-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
        .append_wal_objects(FOLD_AT_WAL_OBJECTS.get())
        .await
        .expect("seed a tail at the fold threshold");
    });
    run_command(async {
        open()
            .await
            .client
            .put_file(
                &NamespacePath::parse("demo", "/file").expect("file path"),
                b"payload",
            )
            .await
            .expect("a write past the fold threshold");
    });
    let tail = run_command(async {
        embedded_host(&open().await)
            .maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("diagnostics")
            .wal_tail_objects
    });
    assert!(
        tail < FOLD_AT_WAL_OBJECTS.get(),
        "the fold did not finish: {tail}"
    );
}

fn embedded_host(target: &ResolvedTarget) -> &crate::backend::MaintenanceHost {
    let Backend::Embedded { host, .. } = &target.backend else {
        panic!("an embedded profile should open an embedded backend");
    };
    host
}

/// Runs `command` as the entry point runs a command: on a Tokio runtime of
/// its own, then drains the runtimes that the command opened.
fn run_command<T>(command: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("command runtime")
        .block_on(crate::backend::run_then_drain(command))
}

#[tokio::test]
async fn an_embedded_fence_is_retried_once_in_a_new_session() {
    let directory = tempfile::tempdir().expect("store directory");
    let config = StoreConfig::LocalFs(LocalFsStoreConfig {
        root: directory.path().display().to_string(),
        key_prefix: None,
    });
    let mut first = ResolvedTarget::embedded(&config, None, TransientRetry::Retry)
        .await
        .expect("first embedded host");
    let mut second = ResolvedTarget::embedded(&config, None, TransientRetry::Retry)
        .await
        .expect("second embedded host");
    first.act_as(loonfs_test_support::test_actor());
    second.act_as(loonfs_test_support::test_actor());
    let path = NamespacePath::parse("demo", "/first").expect("path");
    first
        .client
        .create_namespace(Some(path.namespace()))
        .await
        .expect("create namespace");
    first
        .client
        .create_directory(&path)
        .await
        .expect("first write");
    let path = NamespacePath::parse("demo", "/second").expect("path");
    second
        .client
        .create_directory(&path)
        .await
        .expect("take over");
    // Each host's session finds the other host's claim, and the host sends
    // the request once more in a new session, whose claim raises the writer
    // epoch.
    let store =
        loonfs_objectstore::local_fs_store::LocalFsStore::new(directory.path()).expect("store");
    let writer_epoch = || async {
        loonfs_core::test_support::namespace_store(&store, path.namespace())
            .load_read_state()
            .await
            .expect("read head")
            .writer_epoch
            .0
    };
    for (host, path) in [(&first, "/after"), (&second, "/after-second")] {
        let before = writer_epoch().await;
        host.client
            .create_directory(&NamespacePath::parse("demo", path).expect("path"))
            .await
            .expect("the fenced request takes the namespace back");
        assert_eq!(writer_epoch().await, before + 1);
    }
}
