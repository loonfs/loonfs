//! Embedded requests without networking or bearer credentials.

use crate::config::StoreConfig;
use crate::resolve::ResolvedTarget;
use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_client::{NamespacePath, PayloadSource};
use loonfs_core::limits::FOLD_AT_WAL_OBJECTS;
use loonfs_types::NamespaceAccess;

#[test]
fn embedded_requests_need_no_socket_or_token_and_stream_past_the_server_body_limit() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("runtime without a networking driver");
    runtime.block_on(async {
        let directory = tempfile::tempdir().expect("store directory");
        let target = ResolvedTarget::embedded(
            &StoreConfig::LocalFs {
                root: directory.path().display().to_string(),
                key_prefix: None,
            },
            None,
            false,
        )
        .await
        .expect("embedded profile without credentials");
        let path = NamespacePath::parse("demo", "/large.bin").expect("path");
        let actor = loonfs_test_support::test_actor();
        target
            .client
            .create_namespace(path.namespace(), &actor, NamespaceAccess::unrestricted())
            .await
            .expect("namespace without a listener");
        let chunk = Bytes::from(vec![42; 1024 * 1024]);
        let source = PayloadSource::stream(
            futures::stream::iter((0..257).map(move |_| Ok(chunk.clone()))).boxed(),
        );
        target
            .client
            .put_file_stream(&path, source, &actor)
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
    let config = StoreConfig::LocalFs {
        root: directory.path().display().to_string(),
        key_prefix: None,
    };
    let namespace_id = loonfs_types::NamespaceId::parse("demo").expect("namespace id");
    let actor = loonfs_test_support::test_actor();
    let open = || async {
        ResolvedTarget::embedded(&config, None, true)
            .await
            .expect("embedded profile")
    };
    run_command(async {
        let target = open().await;
        target
            .client
            .create_namespace(&namespace_id, &actor, NamespaceAccess::unrestricted())
            .await
            .expect("namespace");
        let store = target
            .maintenance
            .as_ref()
            .expect("embedded host")
            .runtime
            .object_store();
        loonfs_core::test_support::append_wal_objects(
            store.as_ref(),
            &namespace_id,
            FOLD_AT_WAL_OBJECTS,
            &loonfs_core::MutationContext {
                writer_id: loonfs_types::WriterId::parse("tail-seed").expect("writer id"),
                now_ms: 1_000,
            },
        )
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
                &actor,
            )
            .await
            .expect("a write past the fold threshold");
    });
    let tail = run_command(async {
        open()
            .await
            .maintenance
            .as_ref()
            .expect("embedded host")
            .maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("diagnostics")
            .wal_tail_objects
    });
    assert!(
        tail < FOLD_AT_WAL_OBJECTS,
        "the fold did not finish: {tail}"
    );
}

fn run_command<T>(command: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("command runtime")
        .block_on(command)
}

#[tokio::test]
async fn an_embedded_fence_fails_the_request_and_a_later_request_starts_a_new_session() {
    let directory = tempfile::tempdir().expect("store directory");
    let config = StoreConfig::LocalFs {
        root: directory.path().display().to_string(),
        key_prefix: None,
    };
    let first = ResolvedTarget::embedded(&config, None, false)
        .await
        .expect("first embedded host");
    let second = ResolvedTarget::embedded(&config, None, false)
        .await
        .expect("second embedded host");
    let actor = loonfs_test_support::test_actor();
    let path = NamespacePath::parse("demo", "/first").expect("path");
    first
        .client
        .create_namespace(path.namespace(), &actor, NamespaceAccess::unrestricted())
        .await
        .expect("create namespace");
    first
        .client
        .create_directory(&path, &actor)
        .await
        .expect("first write");
    let path = NamespacePath::parse("demo", "/second").expect("path");
    second
        .client
        .create_directory(&path, &actor)
        .await
        .expect("take over");
    let path = NamespacePath::parse("demo", "/after").expect("path");
    let error = first
        .client
        .create_directory(&path, &actor)
        .await
        .expect_err("fenced request");
    assert_eq!(error.code(), Some(loonfs_types::ErrorCode::WriterFenced));
    first
        .client
        .create_directory(&path, &actor)
        .await
        .expect("later request");
}
